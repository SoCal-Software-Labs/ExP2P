use {
    bytes::Bytes,
    qp2p::{Config, Connection, ConnectionIncoming, Endpoint, SendStream},
    rustler::{types::Pid, Atom, Binary, Encoder, NifResult, Term},
    std::{io::Write, net::SocketAddr, sync::Arc, time::Duration},
    tokio::{
        runtime::Builder,
        sync::{mpsc, Mutex},
        time::timeout,
    },
    futures::future::try_join_all
};

rustler::atoms! {
    ok,
    error,
    new_endpoint,
    new_connection,
    new_connection_to_accept,
    new_message,
    message_reply
}

const CONNECTION_SEND_BUFFER_SIZE: usize = 1000;
const STREAM_SEND_BUFFER_SIZE: usize = 1000;

#[derive(Clone)]
pub struct StreamResource {
    pub stream: Arc<Mutex<SendStream>>,
}

pub struct StreamResourceArc {
    pub inner: StreamResource,
}

pub struct EndpointResource {
    pub endpoint: Endpoint,
    pub stream_sender: mpsc::Sender<(Vec<u8>, StreamResource, Pid, u64)>,
    pub unidirectional_sender: mpsc::Sender<(Vec<u8>, Connection, Pid, u64)>,
    pub unidirectional_many_sender: mpsc::Sender<(Vec<u8>, Vec<Vec<u8>>, Pid, u64)>,
    pub bidirectional_sender: mpsc::Sender<(Vec<u8>, Connection, Pid, u64)>,
    pub new_connection_sender: mpsc::Sender<(Vec<Vec<u8>>, Pid, u64)>,
}

pub struct ConnectionResource {
    pub connection: Connection,
}

pub struct ResponderResource {
    pub sender: mpsc::Sender<Pid>,
}

#[rustler::nif]
fn connect_to_peer(
    endpoint_term: Term,
    bytes: Vec<Binary>,
    waiting: Pid,
    timeout: u64,
) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;

    let ret = endpoint.new_connection_sender.try_send((
        bytes.iter().map(|b| b.as_slice().to_vec()).collect(),
        waiting,
        timeout,
    ));

    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn set_controlling_pid(responder_term: Term, owner: Pid) -> NifResult<Atom> {
    let responder: rustler::ResourceArc<ResponderResource> = responder_term.decode()?;

    let ret = responder.sender.try_send(owner);

    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn send_unidirectional_many(
    endpoint_term: Term,
    peers: Vec<Binary>,
    bytes: Binary,
    waiting: Pid,
    timeout: u64,
) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;
    let converted = peers
        .iter()
        .map(|b| b.as_slice().to_vec())
        .collect::<Vec<Vec<u8>>>();

    let ret = endpoint.unidirectional_many_sender.try_send((
        bytes.as_slice().to_vec(),
        converted,
        waiting,
        timeout,
    ));

    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn send_unidirectional(
    endpoint_term: Term,
    connection_term: Term,
    bytes: Binary,
    waiting: Pid,
    timeout: u64,
) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;
    let connection: rustler::ResourceArc<ConnectionResource> = connection_term.decode()?;

    let ret = endpoint.unidirectional_sender.try_send((
        bytes.as_slice().to_vec(),
        connection.connection.clone(),
        waiting,
        timeout,
    ));

    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn send_bidirectional(
    endpoint_term: Term,
    connection_term: Term,
    bytes: Binary,
    waiting: Pid,
    timeout: u64,
) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;
    let connection: rustler::ResourceArc<ConnectionResource> = connection_term.decode()?;

    let ret = endpoint.bidirectional_sender.try_send((
        bytes.as_slice().to_vec(),
        connection.connection.clone(),
        waiting,
        timeout,
    ));
    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn send_stream_response(
    endpoint_term: Term,
    stream_term: Term,
    bytes: Binary,
    waiting: Pid,
    timeout: u64,
) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;
    let stream: rustler::ResourceArc<StreamResourceArc> = stream_term.decode()?;

    let ret = endpoint.stream_sender.try_send((
        bytes.as_slice().to_vec(),
        stream.inner.clone(),
        waiting,
        timeout,
    ));
    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn start(listening: Pid, bind_address: Binary, bootstrap: Vec<Binary>) -> Atom {
    let converted = bootstrap
        .iter()
        .filter_map(|b| String::from_utf8_lossy(b.as_slice()).parse().ok())
        .collect::<Vec<SocketAddr>>();
    let bind = String::from_utf8_lossy(bind_address.as_slice())
        .parse::<SocketAddr>()
        .expect("Invalid binding address");

    std::thread::spawn(move || {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                match run_loop(listening, bind, converted.as_slice()).await {
                    Ok(_) => (),
                    Err(e) => {
                        let mut msg_env = rustler::OwnedEnv::new();
                        msg_env.send_and_clear(&listening, |env| {
                            (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                        });
                    }
                }
            })
    });

    ok()
}

async fn run_birectional_send(
    msg: Vec<u8>,
    connection: Connection,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut other_send_stream, mut recv_stream) = connection.open_bi().await?;
    other_send_stream.send_user_msg(Bytes::from(msg)).await?;
    let reply = timeout(Duration::from_millis(timeout_val), recv_stream.next()).await??;

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&waiting, |env| {
        (message_reply(), make_binary(env, &reply[..])).encode(env)
    });
    Ok(())
}

async fn run_unidirectional_send(
    msg: Vec<u8>,
    connection: Connection,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut other_send_stream = connection.open_uni().await?;
    timeout(
        Duration::from_millis(timeout_val),
        other_send_stream.send_user_msg(Bytes::from(msg)),
    )
    .await??;

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&waiting, |env| ok().encode(env));

    Ok(())
}

async fn run_unidirectional_many_send(
    endpoint: Endpoint,
    msg: Vec<u8>,
    socket_addrs: Vec<Vec<u8>>,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let byte_msg = Bytes::from(msg);

    let converted = socket_addrs
        .iter()
        .filter_map(|b| String::from_utf8_lossy(b).parse().ok())
        .collect::<Vec<SocketAddr>>();

    let handles = converted.iter().map(|addr| {
        let e2 = endpoint.clone();
        let b2 = byte_msg.clone();
        async move {
            if let Ok((conn, _)) = e2.connect_to(&addr.clone()).await {
                conn.send(b2).await
            } else {
                Ok(())
            }
        }
    });

    timeout(
        Duration::from_millis(timeout_val),
        try_join_all(handles),
    )
    .await??;

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&waiting, |env| ok().encode(env));

    Ok(())
}

async fn run_response_send(
    msg: Vec<u8>,
    stream: StreamResource,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    timeout(
        Duration::from_millis(timeout_val),
        stream.stream.lock().await.send_user_msg(Bytes::from(msg)),
    )
    .await??;

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&waiting, |env| ok().encode(env));

    Ok(())
}

async fn run_connect(
    endpoint: &Endpoint,
    peers: Vec<Vec<u8>>,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let converted = peers
        .iter()
        .filter_map(|b| String::from_utf8_lossy(b.as_slice()).parse().ok())
        .collect::<Vec<SocketAddr>>();

    if let Some((conn, _)) = timeout(
        Duration::from_millis(timeout_val),
        endpoint.connect_to_any(converted.as_slice()),
    )
    .await?
    {
        let mut msg_env = rustler::OwnedEnv::new();
        msg_env.send_and_clear(&waiting, |env| {
            (
                new_connection(),
                rustler::ResourceArc::new(ConnectionResource { connection: conn }),
            )
                .encode(env)
        });
    }

    Ok(())
}

async fn run_loop(
    listening: Pid,
    bind_address: SocketAddr,
    bootstrap: &[SocketAddr],
) -> Result<(), Box<dyn std::error::Error>> {
    let (node, mut incoming_conns, _contact) = Endpoint::new_peer(
        // bind_address 127.0.0.1:0
        bind_address,
        bootstrap,
        Config {
            idle_timeout: Duration::from_secs(60 * 60).into(), // 1 hour idle timeout.
            ..Default::default()
        },
    )
    .await?;

    let (bidirectional_sender, mut bidirectional_rx) =
        mpsc::channel::<(Vec<u8>, Connection, Pid, u64)>(CONNECTION_SEND_BUFFER_SIZE);
    let (tx, mut rx) =
        mpsc::channel::<(Vec<u8>, Connection, Pid, u64)>(CONNECTION_SEND_BUFFER_SIZE);
    let (stream_tx, mut stream_rx) =
        mpsc::channel::<(Vec<u8>, StreamResource, Pid, u64)>(STREAM_SEND_BUFFER_SIZE);
    let (connect_tx, mut connect_rx) =
        mpsc::channel::<(Vec<Vec<u8>>, Pid, u64)>(STREAM_SEND_BUFFER_SIZE);
    let (many_tx, mut many_rx) =
        mpsc::channel::<(Vec<u8>, Vec<Vec<u8>>, Pid, u64)>(STREAM_SEND_BUFFER_SIZE);

    let node2 = node.clone();
    tokio::spawn(async move {
        while let Some((msg, waiting, timeout_val)) = connect_rx.recv().await {
            let e3 = node2.clone();
            tokio::spawn(async move {
                match run_connect(&e3, msg, waiting, timeout_val).await {
                    Ok(_) => (),
                    Err(e) => {
                        let mut msg_env = rustler::OwnedEnv::new();
                        msg_env.send_and_clear(&waiting, |env| {
                            (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                        });
                    }
                }
            });
        }
    });

    tokio::spawn(async move {
        while let Some((msg, connection, waiting, timeout)) = bidirectional_rx.recv().await {
            tokio::spawn(async move {
                match run_birectional_send(msg, connection, waiting, timeout).await {
                    Ok(_) => (),
                    Err(e) => {
                        let mut msg_env = rustler::OwnedEnv::new();
                        msg_env.send_and_clear(&waiting, |env| {
                            (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                        });
                    }
                }
            });
        }
    });

    tokio::spawn(async move {
        while let Some((msg, sending_resource, waiting, timeout)) = stream_rx.recv().await {
            tokio::spawn(async move {
                match run_response_send(msg, sending_resource, waiting, timeout).await {
                    Ok(_) => (),
                    Err(e) => {
                        let mut msg_env = rustler::OwnedEnv::new();
                        msg_env.send_and_clear(&waiting, |env| {
                            (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                        });
                    }
                }
            });
        }
    });

    tokio::spawn(async move {
        while let Some((msg, connection, waiting, timeout)) = rx.recv().await {
            tokio::spawn(async move {
                match run_unidirectional_send(msg, connection, waiting, timeout).await {
                    Ok(_) => (),
                    Err(e) => {
                        let mut msg_env = rustler::OwnedEnv::new();
                        msg_env.send_and_clear(&waiting, |env| {
                            (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                        });
                    }
                }
            });
        }
    });

    let node4 = node.clone();
    tokio::spawn(async move {
        while let Some((msg, socketaddrs, waiting, timeout)) = many_rx.recv().await {
            let e3 = node4.clone();
            tokio::spawn(async move {
                match run_unidirectional_many_send(e3.clone(), msg, socketaddrs, waiting, timeout).await {
                    Ok(_) => (),
                    Err(e) => {
                        let mut msg_env = rustler::OwnedEnv::new();
                        msg_env.send_and_clear(&waiting, |env| {
                            (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                        });
                    }
                }
            });
        }
    });

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&listening, |env| {
        (
            new_endpoint(),
            rustler::ResourceArc::new(EndpointResource {
                endpoint: node.clone(),
                unidirectional_sender: tx,
                unidirectional_many_sender: many_tx,
                stream_sender: stream_tx,
                bidirectional_sender: bidirectional_sender,
                new_connection_sender: connect_tx,
            }),
            make_binary(env, node.public_addr().to_string().as_bytes()),
        )
            .encode(env)
    });

    while let Some((connection, mut incoming_messages)) = incoming_conns.next().await {
        tokio::spawn(async move {
            match run_loop_connection(listening, connection, &mut incoming_messages).await {
                Ok(_) => (),
                Err(e) => {
                    let mut msg_env = rustler::OwnedEnv::new();
                    msg_env.send_and_clear(&listening, |env| {
                        (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                    });
                }
            }
        });
    }

    Ok(())
}

async fn run_loop_connection<'a>(
    listening: Pid,
    connection: Connection,
    incoming_messages: &mut ConnectionIncoming,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn_string = connection.remote_address().to_string();
    let conn_bytes = conn_string.as_bytes();
    let (tx, mut rx) = mpsc::channel(1);

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&listening, |env| {
        (
            new_connection_to_accept(),
            rustler::ResourceArc::new(ConnectionResource {
                connection: connection.clone(),
            }),
            rustler::ResourceArc::new(ResponderResource { sender: tx }),
        )
            .encode(env)
    });
    if let Some(pid) = rx.recv().await {
        rx.close();

        while let Some((bytes, maybe_stream)) = incoming_messages.next_with_stream().await? {
            msg_env.send_and_clear(&pid, |env| {
                (
                    new_message(),
                    make_binary(env, &bytes[..]),
                    maybe_stream.map(|tx| {
                        rustler::ResourceArc::new(StreamResourceArc {
                            inner: StreamResource { stream: tx },
                        })
                    }),
                    make_binary(env, conn_bytes.clone())
                )
                    .encode(env)
            });
        }
    }

    Ok(())
}

fn make_binary<'a>(env: rustler::Env<'a>, bytes: &[u8]) -> rustler::Binary<'a> {
    let mut bin = match rustler::OwnedBinary::new(bytes.len()) {
        Some(bin) => bin,
        None => panic!("binary term allocation fail"),
    };
    bin.as_mut_slice()
        .write_all(&bytes[..])
        .expect("memory copy of string failed");

    bin.release(env)
}

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    rustler::resource!(StreamResourceArc, env);
    rustler::resource!(EndpointResource, env);
    rustler::resource!(ConnectionResource, env);
    rustler::resource!(ResponderResource, env);
    true
}

rustler::init!(
    "Elixir.ExP2P",
    [
        start,
        set_controlling_pid,
        connect_to_peer,
        send_unidirectional,
        send_unidirectional_many,
        send_bidirectional,
        send_stream_response
    ],
    load = load
);
