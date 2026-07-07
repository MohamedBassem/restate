use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use futures::Stream;
use slab::Slab;

pub trait LaneScheduler {
    fn register(&mut self, id: usize);
    fn unregister(&mut self, id: usize);
    fn on_item_ready<T>(&mut self, id: usize, item: &T);
    fn next(&mut self) -> Option<usize>;
}

pub struct LaneSender<T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    id: usize,
    inner: Arc<Mutex<Inner<T, Id, Scheduler>>>,
}

impl<T, Id, Scheduler> LaneSender<T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    pub fn send(&self, item: T) -> SendFut<'_, T, Id, Scheduler> {
        SendFut {
            sender: self,
            item: Some(item),
        }
    }

    pub fn reserve(&self) -> ReserveFut<'_, T, Id, Scheduler> {
        ReserveFut { sender: self }
    }
}

pub struct SendFut<'a, T, Id, Scheduler: LaneScheduler> {
    sender: &'a LaneSender<T, Id, Scheduler>,
    item: Option<T>,
}

impl<'a, T, Id, Scheduler: LaneScheduler> Future for SendFut<'a, T, Id, Scheduler>
where
    T: Unpin,
{
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let mut g = this.sender.inner.lock().unwrap();
        let Inner {
            lanes,
            scheduler,
            waker,
            ..
        } = &mut *g;

        let Some(lane) = lanes.get_mut(this.sender.id) else {
            panic!("Lane {} is not open", this.sender.id);
        };
        match &mut lane.entry {
            Entry::Ready(_) | Entry::Reserved => {
                lane.parked_senders.push(cx.waker().clone());
                std::task::Poll::Pending
            }
            Entry::Empty => {
                let item = this.item.take().expect("polled after completion");
                scheduler.on_item_ready(this.sender.id, &item);
                lane.entry = Entry::Ready(item);
                let waker = waker.take();
                if let Some(waker) = waker {
                    waker.wake();
                }
                std::task::Poll::Ready(())
            }
        }
    }
}

pub struct Permit<'a, T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    sender: &'a LaneSender<T, Id, Scheduler>,
}

impl<'a, T, Id, Scheduler> Permit<'a, T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    pub fn send(self, item: T) {
        let mut g = self.sender.inner.lock().unwrap();
        let Inner {
            lanes,
            scheduler,
            waker,
            ..
        } = &mut *g;

        let lane = lanes.get_mut(self.sender.id).unwrap();
        debug_assert!(matches!(lane.entry, Entry::Reserved));
        scheduler.on_item_ready(self.sender.id, &item);
        lane.entry = Entry::Ready(item);
        let waker = waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<'a, T, Id, Scheduler> Drop for Permit<'a, T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    fn drop(&mut self) {
        // If the permit was consumed via `send`, the lane is already `Ready` and there is
        // nothing to release. Otherwise the reservation is dropped unused: free the slot so
        // the lane doesn't stay wedged in `Reserved` (which would also leak the lane, since
        // `LaneSender::drop` only reclaims `Empty` lanes), and wake any parked senders that
        // were waiting for the slot.
        let mut g = self.sender.inner.lock().unwrap();
        if let Some(lane) = g.lanes.get_mut(self.sender.id)
            && matches!(lane.entry, Entry::Reserved)
        {
            lane.entry = Entry::Empty;
            for waker in lane.parked_senders.drain(..) {
                waker.wake();
            }
        }
    }
}

pub struct ReserveFut<'a, T, Id, Scheduler: LaneScheduler> {
    sender: &'a LaneSender<T, Id, Scheduler>,
}

impl<'a, T, Id, Scheduler: LaneScheduler> Future for ReserveFut<'a, T, Id, Scheduler>
where
    T: Unpin,
{
    type Output = Permit<'a, T, Id, Scheduler>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let mut g = this.sender.inner.lock().unwrap();
        let Inner { lanes, waker, .. } = &mut *g;

        let Some(lane) = lanes.get_mut(this.sender.id) else {
            panic!("Lane {} is not open", this.sender.id);
        };
        match &mut lane.entry {
            Entry::Ready(_) | Entry::Reserved => {
                lane.parked_senders.push(cx.waker().clone());
                std::task::Poll::Pending
            }
            Entry::Empty => {
                lane.entry = Entry::Reserved;
                let waker = waker.take();
                if let Some(waker) = waker {
                    waker.wake();
                }
                std::task::Poll::Ready(Permit {
                    sender: this.sender,
                })
            }
        }
    }
}

impl<T, Id, Scheduler> Clone for LaneSender<T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    fn clone(&self) -> Self {
        self.inner.lock().unwrap().lanes[self.id].num_senders += 1;

        Self {
            id: self.id,
            inner: self.inner.clone(),
        }
    }
}

impl<T, Id, Scheduler> Drop for LaneSender<T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    fn drop(&mut self) {
        let mut g = self.inner.lock().unwrap();
        let Inner {
            lanes,
            scheduler,
            id_map,
            ..
        } = &mut *g;

        let Some(lane) = lanes.get_mut(self.id) else {
            panic!("Lane {} is not open", self.id);
        };

        lane.num_senders -= 1;
        if lane.num_senders == 0 && matches!(lanes[self.id].entry, Entry::Empty) {
            lanes.remove(self.id);
            scheduler.unregister(self.id);
            id_map.retain(|_, v| *v != self.id);
        }
    }
}

struct Inner<T, Id, Scheduler> {
    lanes: Slab<Lane<T>>,
    scheduler: Scheduler,
    id_map: HashMap<Id, usize>,
    waker: Option<Waker>,
}

impl<T, Id, Scheduler> Inner<T, Id, Scheduler> {
    pub fn new(scheduler: Scheduler) -> Self {
        Self {
            lanes: Slab::new(),
            scheduler,
            id_map: HashMap::new(),
            waker: None,
        }
    }
}

enum Entry<T> {
    Empty,
    #[allow(dead_code)]
    Reserved,
    Ready(T),
}

struct Lane<T> {
    entry: Entry<T>,
    num_senders: usize,
    parked_senders: Vec<Waker>,
}

impl<T> Lane<T> {
    pub fn new() -> Self {
        Self {
            entry: Entry::Empty,
            num_senders: 0,
            parked_senders: Vec::new(),
        }
    }
}

pub struct FairLaneHandle<T, Id, Scheduler> {
    inner: Arc<Mutex<Inner<T, Id, Scheduler>>>,
}

impl<T, Id, Scheduler> FairLaneHandle<T, Id, Scheduler>
where
    Id: Eq + Hash,
    Scheduler: LaneScheduler,
{
    pub fn open_lane(&self, id: Id) -> LaneSender<T, Id, Scheduler> {
        let mut g = self.inner.lock().unwrap();
        let Inner {
            lanes,
            id_map,
            scheduler,
            ..
        } = &mut *g;

        let internal_id = *id_map.entry(id).or_insert_with(|| {
            let lane_id = lanes.insert(Lane::new());
            scheduler.register(lane_id);
            lane_id
        });
        lanes[internal_id].num_senders += 1;
        LaneSender {
            id: internal_id,
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct FairLaneQueue<T, Id, Scheduler> {
    inner: Arc<Mutex<Inner<T, Id, Scheduler>>>,
}

impl<T, Id, Scheduler> FairLaneQueue<T, Id, Scheduler> {
    pub fn new(scheduler: Scheduler) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new(scheduler))),
        }
    }

    pub fn handle(&self) -> FairLaneHandle<T, Id, Scheduler> {
        FairLaneHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, Id, Scheduler> Stream for FairLaneQueue<T, Id, Scheduler>
where
    Scheduler: LaneScheduler,
{
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut();
        let mut g = this.inner.lock().unwrap();
        let Inner {
            lanes,
            scheduler,
            waker,
            id_map,
            ..
        } = &mut *g;
        *waker = Some(cx.waker().clone());

        if let Some(id) = scheduler.next() {
            let lane = &mut lanes[id];
            let Entry::Ready(item) = std::mem::replace(&mut lane.entry, Entry::Empty) else {
                panic!("Lane {} is ready but has no item", id);
            };
            // Wake up all parked senders. This is needed in case one of them
            // was cancelled.
            for waker in lane.parked_senders.drain(..) {
                waker.wake();
            }
            if lane.num_senders == 0 {
                lanes.remove(id);
                scheduler.unregister(id);
                id_map.retain(|_, v| *v != id);
            }
            return std::task::Poll::Ready(Some(item));
        }
        std::task::Poll::Pending
    }
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
enum RoundRobinSchedulerStatus {
    #[default]
    Empty,
    Staging,
    Ready,
}

pub type RoundRobinLaneScheduler<T, Id> = FairLaneQueue<T, Id, RoundRobinScheduler>;

pub struct RoundRobinScheduler {
    items: Vec<RoundRobinSchedulerStatus>,
    queue: VecDeque<usize>,
}

impl RoundRobinScheduler {
    pub fn new() -> Self {
        Self {
            items: Vec::with_capacity(1024),
            queue: VecDeque::new(),
        }
    }
}

impl Default for RoundRobinScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl LaneScheduler for RoundRobinScheduler {
    fn register(&mut self, id: usize) {
        if id >= self.items.len() {
            self.items.resize(id + 1, RoundRobinSchedulerStatus::Empty);
        }
        debug_assert_eq!(self.items[id], RoundRobinSchedulerStatus::Empty);
        self.items[id] = RoundRobinSchedulerStatus::Staging;
    }

    fn unregister(&mut self, id: usize) {
        // We'd never unregister an item that's ready until it's consumed
        debug_assert_eq!(self.items[id], RoundRobinSchedulerStatus::Staging);
        self.items[id] = RoundRobinSchedulerStatus::Empty;
    }

    fn on_item_ready<T>(&mut self, id: usize, _item: &T) {
        let item = &mut self.items[id];
        if item == &RoundRobinSchedulerStatus::Ready {
            // We already have this item marked as ready
            return;
        }
        *item = RoundRobinSchedulerStatus::Ready;
        self.queue.push_back(id);
    }

    fn next(&mut self) -> Option<usize> {
        let val = self.queue.pop_front();
        if let Some(id) = val {
            self.items[id] = RoundRobinSchedulerStatus::Staging;
        }
        val
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn round_robin_queue() {
        let scheduelr = RoundRobinScheduler::new();
        let mut queue = FairLaneQueue::new(scheduelr);
        let handle = queue.handle();

        let tx1 = handle.open_lane(1u32);
        let tx2 = handle.open_lane(2u32);

        tokio::spawn(async move {
            tx1.send(1).await;
            tx1.send(2).await;
            tx1.send(3).await;
        });

        tokio::spawn(async move {
            tx2.send(1).await;
            tx2.send(2).await;
            tx2.send(3).await;
        });

        assert_eq!(queue.next().await, Some(1));
        assert_eq!(queue.next().await, Some(1));
        assert_eq!(queue.next().await, Some(2));
        assert_eq!(queue.next().await, Some(2));
        assert_eq!(queue.next().await, Some(3));
        assert_eq!(queue.next().await, Some(3));
    }

    #[tokio::test]
    async fn reseve() {
        let scheduelr = RoundRobinScheduler::new();
        let mut queue = FairLaneQueue::new(scheduelr);
        let handle = queue.handle();

        let tx1_1 = handle.open_lane(1u32);
        let tx1_2 = handle.open_lane(1u32);

        let p1 = tx1_1.reserve().await;
        tokio::spawn(async move {
            let p2 = tx1_2.reserve().await;
            p2.send(2);
        });

        p1.send(1);

        assert_eq!(queue.next().await, Some(1));
        assert_eq!(queue.next().await, Some(2));
    }
}
