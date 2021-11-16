use std::collections::{HashMap, HashSet, VecDeque};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use actix::{Actor, ActorContext, Context, Handler, MailboxError, Message};
use futures::future::BoxFuture;
use futures::{future, FutureExt};
use once_cell::sync::Lazy;
use rand::{thread_rng, RngCore};
use tracing::debug;

use near_crypto::{KeyType, SecretKey};
use near_primitives::hash::hash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochId;
use near_primitives::utils::index_to_bytes;

use crate::types::{
    NetworkInfo, PeerInfo, PeerManagerMessageRequest, PeerManagerMessageResponse, ReasonForBan,
};
use crate::{NetworkResponses, PeerManagerActor, PeerManagerAdapter};

static OPENED_PORTS: Lazy<Mutex<HashSet<u16>>> = Lazy::new(|| Mutex::new(HashSet::new()));

/// Returns available port.
pub fn open_port() -> u16 {
    // Use port 0 to allow the OS to assign an open port.
    // TcpListener's Drop impl will unbind the port as soon as listener goes out of scope.
    // We retry multiple times and store selected port in OPENED_PORTS to avoid port collision among
    // multiple tests.
    let max_attempts = 100;

    for _ in 0..max_attempts {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let mut opened_ports = OPENED_PORTS.lock().unwrap();

        if !opened_ports.contains(&port) {
            opened_ports.insert(port);
            return port;
        }
    }

    panic!("Failed to find an open port after {} attempts.", max_attempts);
}

// `peer_id_from_seed` generate `PeerId` from seed for unit tests
pub fn peer_id_from_seed(seed: &str) -> PeerId {
    SecretKey::from_seed(KeyType::ED25519, seed).public_key().into()
}

// `convert_boot_nodes` generate list of `PeerInfos` for unit tests
pub fn convert_boot_nodes(boot_nodes: Vec<(&str, u16)>) -> Vec<PeerInfo> {
    let mut result = vec![];
    for (peer_seed, port) in boot_nodes {
        let id = peer_id_from_seed(peer_seed);
        result.push(PeerInfo::new(id.into(), format!("127.0.0.1:{}", port).parse().unwrap()))
    }
    result
}

/// Timeouts by stopping system without any condition and raises panic.
/// Useful in tests to prevent them from running forever.
#[allow(unreachable_code)]
pub fn wait_or_panic(max_wait_ms: u64) {
    actix::spawn(tokio::time::sleep(Duration::from_millis(max_wait_ms)).then(|_| {
        panic!("Timeout exceeded.");
        future::ready(())
    }));
}

/// Waits until condition or timeouts with panic.
/// Use in tests to check for a condition and stop or fail otherwise.
///
/// # Example
///
/// ```
/// use actix::{System, Actor};
/// use near_network::test_utils::WaitOrTimeoutActor;
/// use std::time::{Instant, Duration};
///
/// near_actix_test_utils::run_actix(async {
///     let start = Instant::now();
///     WaitOrTimeoutActor::new(
///         Box::new(move |ctx| {
///             if start.elapsed() > Duration::from_millis(10) {
///                 System::current().stop()
///             }
///         }),
///         1000,
///         60000,
///     ).start();
/// });
/// ```
pub struct WaitOrTimeoutActor {
    f: Box<dyn FnMut(&mut Context<WaitOrTimeoutActor>)>,
    check_interval_ms: u64,
    max_wait_ms: u64,
    ms_slept: u64,
}

impl WaitOrTimeoutActor {
    pub fn new(
        f: Box<dyn FnMut(&mut Context<WaitOrTimeoutActor>)>,
        check_interval_ms: u64,
        max_wait_ms: u64,
    ) -> Self {
        WaitOrTimeoutActor { f, check_interval_ms, max_wait_ms, ms_slept: 0 }
    }

    fn wait_or_timeout(&mut self, ctx: &mut Context<Self>) {
        (self.f)(ctx);

        near_performance_metrics::actix::run_later(
            ctx,
            Duration::from_millis(self.check_interval_ms),
            move |act, ctx| {
                act.ms_slept += act.check_interval_ms;
                if act.ms_slept > act.max_wait_ms {
                    println!("BBBB Slept {}; max_wait_ms {}", act.ms_slept, act.max_wait_ms);
                    panic!("Timed out waiting for the condition");
                }
                act.wait_or_timeout(ctx);
            },
        );
    }
}

impl Actor for WaitOrTimeoutActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.wait_or_timeout(ctx);
    }
}

// Converts Vec<&str> to Vec<String>
pub fn vec_ref_to_str(values: Vec<&str>) -> Vec<String> {
    values.into_iter().map(|x| x.to_string()).collect()
}

// Gets random PeerId
pub fn random_peer_id() -> PeerId {
    let sk = SecretKey::from_random(KeyType::ED25519);
    sk.public_key().into()
}

// Gets random EpochId
pub fn random_epoch_id() -> EpochId {
    EpochId(hash(index_to_bytes(thread_rng().next_u64()).as_ref()))
}

// Compare whenever routing table match.
pub fn expected_routing_tables(
    current: HashMap<PeerId, Vec<PeerId>>,
    expected: Vec<(PeerId, Vec<PeerId>)>,
) -> bool {
    if current.len() != expected.len() {
        return false;
    }

    for (peer, paths) in expected.into_iter() {
        let cur_paths = current.get(&peer);
        if !cur_paths.is_some() {
            return false;
        }
        let cur_paths = cur_paths.unwrap();
        if cur_paths.len() != paths.len() {
            return false;
        }
        for next_hop in paths.into_iter() {
            if !cur_paths.contains(&next_hop) {
                return false;
            }
        }
    }

    true
}

/// `GetInfo` gets `NetworkInfo` from `PeerManager`.
pub struct GetInfo {}

impl Message for GetInfo {
    type Result = NetworkInfo;
}

impl Handler<GetInfo> for PeerManagerActor {
    type Result = NetworkInfo;

    fn handle(&mut self, _msg: GetInfo, _ctx: &mut Context<Self>) -> Self::Result {
        self.get_network_info()
    }
}

// `StopSignal is used to stop PeerManagerActor for unit tests
#[derive(Message)]
#[rtype(result = "()")]
pub struct StopSignal {
    pub should_panic: bool,
}

impl StopSignal {
    pub fn new() -> Self {
        Self { should_panic: false }
    }

    pub fn should_panic() -> Self {
        Self { should_panic: true }
    }
}

impl Handler<StopSignal> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: StopSignal, ctx: &mut Self::Context) -> Self::Result {
        debug!(target: "network", "Receive Stop Signal.");

        if msg.should_panic {
            panic!("Node crashed");
        } else {
            ctx.stop();
        }
    }
}

/// Ban peer for unit tests.
/// Calls `try_ban_peer` in `PeerManagerActor`.
#[derive(Message)]
#[rtype(result = "()")]
pub struct BanPeerSignal {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

impl BanPeerSignal {
    pub fn new(peer_id: PeerId) -> Self {
        Self { peer_id, ban_reason: ReasonForBan::None }
    }
}

impl Handler<BanPeerSignal> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: BanPeerSignal, ctx: &mut Self::Context) -> Self::Result {
        debug!(target: "network", "Ban peer: {:?}", msg.peer_id);
        self.try_ban_peer(ctx, &msg.peer_id, msg.ban_reason);
    }
}

// Mocked `PeerManager` adapter, has a queue of `PeerManagerMessageRequest` messages.
#[derive(Default)]
pub struct MockPeerManagerAdapter {
    pub requests: Arc<RwLock<VecDeque<PeerManagerMessageRequest>>>,
}

impl PeerManagerAdapter for MockPeerManagerAdapter {
    fn send(
        &self,
        msg: PeerManagerMessageRequest,
    ) -> BoxFuture<'static, Result<PeerManagerMessageResponse, MailboxError>> {
        self.do_send(msg);
        future::ok(PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse))
            .boxed()
    }

    fn do_send(&self, msg: PeerManagerMessageRequest) {
        self.requests.write().unwrap().push_back(msg);
    }
}

impl MockPeerManagerAdapter {
    pub fn pop(&self) -> Option<PeerManagerMessageRequest> {
        self.requests.write().unwrap().pop_front()
    }
}
