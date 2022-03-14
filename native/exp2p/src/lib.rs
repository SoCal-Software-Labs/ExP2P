use {
    bytes::Bytes,
    futures::future::try_join_all,
    qp2p::{Config, Connection, ConnectionIncoming, Endpoint, RecvStream, SendStream},
    rustler::{types::Pid, Atom, Binary, Encoder, NifResult, Term},
    std::{io::Write, net::SocketAddr, sync::Arc, time::Duration},
    tokio::{
        runtime::Builder,
        sync::{mpsc, Mutex},
        time::timeout,
    },
};

rustler::atoms! {
    ok,
    error,
    new_endpoint,
    new_connection,
    connection_stopped,
    new_connection_to_accept,
    new_message,
    new_stream,
    new_stream_message,
    stream_finished,
    message_reply
}

const BUFFER_SIZE: usize = 1000;

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
    pub bidirectional_many_sender: mpsc::Sender<(Vec<u8>, Vec<Vec<u8>>, Pid, u64)>,
    pub new_connection_sender: mpsc::Sender<(Vec<Vec<u8>>, Pid, u64)>,
    pub open_bi_sender: mpsc::Sender<(Connection, Pid, Pid)>,
    pub open_pbi_sender: mpsc::Sender<(Connection, Pid, Pid)>,
    pub finish_sender: mpsc::Sender<StreamResource>,
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
fn send_bidirectional_many(
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

    let ret = endpoint.bidirectional_many_sender.try_send((
        bytes.as_slice().to_vec(),
        converted,
        waiting,
        timeout,
    ));
    match ret {
        Ok(_) => Ok(ok()),
        Err(_e) =>{
            Ok(error())
        }
    }
}

#[rustler::nif]
fn send_bidirectional_open(
    endpoint_term: Term,
    connection_term: Term,
    waiting: Pid,
    listener_pid: Pid,
) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;
    let connection: rustler::ResourceArc<ConnectionResource> = connection_term.decode()?;

    let ret =
        endpoint
            .open_bi_sender
            .try_send((connection.connection.clone(), waiting, listener_pid));

    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn send_pseudo_bidirectional_open(
    endpoint_term: Term,
    connection_term: Term,
    waiting: Pid,
    listener_pid: Pid,
) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;
    let connection: rustler::ResourceArc<ConnectionResource> = connection_term.decode()?;

    let ret =
        endpoint
            .open_pbi_sender
            .try_send((connection.connection.clone(), waiting, listener_pid));

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
fn send_stream_finish(endpoint_term: Term, stream_term: Term) -> NifResult<Atom> {
    let endpoint: rustler::ResourceArc<EndpointResource> = endpoint_term.decode()?;
    let stream: rustler::ResourceArc<StreamResourceArc> = stream_term.decode()?;

    let ret = endpoint.finish_sender.try_send(stream.inner.clone());
    match ret {
        Ok(_) => Ok(ok()),
        Err(_) => Ok(error()),
    }
}

#[rustler::nif]
fn start(listening: Pid, bind_address: Binary, bootstrap: Vec<Binary>, client_mode: bool, forward_port: bool) -> Atom {
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
                match run_loop(listening, bind, converted.as_slice(), client_mode, forward_port).await {
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

async fn run_bidirectional_send(
    msg: Vec<u8>,
    connection: Connection,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut other_send_stream, mut recv_stream) = connection.open_bi().await?;
    other_send_stream.send_user_msg(Bytes::from(msg)).await?;

    let reply = timeout(Duration::from_millis(timeout_val), recv_stream.next()).await??;
    other_send_stream.finish().await?;

    if let Some(b) = reply {
        let mut msg_env = rustler::OwnedEnv::new();
        msg_env.send_and_clear(&waiting, |env| {
            (message_reply(), make_binary(env, &b[..])).encode(env)
        });
    }

    Ok(())
}

async fn run_bidirectional_many_send(
    endpoint: Endpoint,
    msg: Vec<u8>,
    addrs: Vec<Vec<u8>>,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {

    let converted = addrs
        .iter()
        .filter_map(|b| String::from_utf8_lossy(b).parse().ok())
        .collect::<Vec<SocketAddr>>();

    if let Some((conn, _)) = timeout(
        Duration::from_millis(timeout_val),
        endpoint.connect_to_any(converted.as_slice()),
    )
    .await?
    {
        let (mut other_send_stream, mut recv_stream) = conn.open_bi().await?;
        other_send_stream.send_user_msg(Bytes::from(msg)).await?;

        let reply = timeout(Duration::from_millis(timeout_val), async {
            recv_stream.next().await
        })
        .await??;
        other_send_stream.finish().await?;

        let conn_str = conn.remote_address().to_string();

        if let Some(b) = reply {
            let mut msg_env = rustler::OwnedEnv::new();
            msg_env.send_and_clear(&waiting, |env| {
                (message_reply(), make_binary(env, conn_str.as_bytes()), make_binary(env, &b[..])).encode(env)
            });
        }

    }

    Ok(())
}

async fn run_bidirectional_open(
    connection: Connection,
    waiting: Pid,
    listener_pid: Pid,
) -> Result<(), Box<dyn std::error::Error>> {
    let (other_send_stream, mut recv_stream) = timeout(Duration::from_millis(5000), async {
        connection.open_bi().await
    })
    .await??;

    let stream_resource = rustler::ResourceArc::new(StreamResourceArc {
        inner: StreamResource {
            stream: Arc::new(Mutex::new(other_send_stream)),
        },
    });

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&waiting, |env| (new_stream(), stream_resource).encode(env));

    while let Ok(Some(bytes)) = recv_stream.next().await {
        msg_env.send_and_clear(&listener_pid, |env| {
            (new_stream_message(), make_binary(env, &bytes[..])).encode(env)
        });
    }

    msg_env.send_and_clear(&listener_pid, |env| (stream_finished()).encode(env));

    Ok(())
}

async fn run_pseudo_bidirectional_open(
    connection: Connection,
    waiting: Pid,
    listener_pid: Pid,
) -> Result<(), Box<dyn std::error::Error>> {
    let (other_send_stream, recv_stream_mutex) = timeout(Duration::from_millis(5000), async {
        connection.open_pseudo_bi().await
    })
    .await??;

    let stream_resource = rustler::ResourceArc::new(StreamResourceArc {
        inner: StreamResource {
            stream: Arc::new(Mutex::new(other_send_stream)),
        },
    });

    let mut msg_env = rustler::OwnedEnv::new();
    msg_env.send_and_clear(&waiting, |env| (new_stream(), stream_resource).encode(env));
    let mut recv_stream = recv_stream_mutex.lock().await;
    while let Ok(Some(bytes)) = recv_stream.next().await {
        msg_env.send_and_clear(&listener_pid, |env| {
            (new_stream_message(), make_binary(env, &bytes[..])).encode(env)
        });
    }

    msg_env.send_and_clear(&listener_pid, |env| (stream_finished()).encode(env));

    Ok(())
}

async fn run_stream_finish(stream: StreamResource) -> Result<(), Box<dyn std::error::Error>> {
    stream.stream.lock().await.finish().await?;

    Ok(())
}

async fn run_unidirectional_send(
    msg: Vec<u8>,
    connection: Connection,
    waiting: Pid,
    timeout_val: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    timeout(
        Duration::from_millis(timeout_val),
        connection.send(Bytes::from(msg)),
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

    timeout(Duration::from_millis(timeout_val), try_join_all(handles)).await??;

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
    client_mode: bool,
    forward_port: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config {
        forward_port: forward_port,
        keep_alive_interval: Duration::from_secs(30).into(),
        idle_timeout: Duration::from_secs(60 * 60).into(), // 1 hour idle timeout.
        ..Default::default()
    };

    let (node, incoming_conns) = 
        if client_mode {
            let node = Endpoint::new_client(
                bind_address,
                config
            )?;
            (node, None)
        } else {
            let (node, incoming_conns, _contact) = Endpoint::new_peer(
                bind_address,
                bootstrap,
                Config {
                    forward_port: forward_port,
                    keep_alive_interval: Duration::from_secs(30).into(),
                    idle_timeout: Duration::from_secs(60 * 60).into(), // 1 hour idle timeout.
                    ..Default::default()
                },
            )
            .await?;
            (node, Some(incoming_conns))
    };

    let (_stop_tx, mut stop_rx) =
        mpsc::channel::<()>(1);
    let (bidirectional_sender, mut bidirectional_rx) =
        mpsc::channel::<(Vec<u8>, Connection, Pid, u64)>(BUFFER_SIZE);
    let (bidirectional_many_sender, mut bidirectional_many_rx) =
        mpsc::channel::<(Vec<u8>, Vec<Vec<u8>>, Pid, u64)>(BUFFER_SIZE);
    let (tx, mut rx) = mpsc::channel::<(Vec<u8>, Connection, Pid, u64)>(BUFFER_SIZE);
    let (stream_tx, mut stream_rx) =
        mpsc::channel::<(Vec<u8>, StreamResource, Pid, u64)>(BUFFER_SIZE);
    let (connect_tx, mut connect_rx) = mpsc::channel::<(Vec<Vec<u8>>, Pid, u64)>(BUFFER_SIZE);
    let (many_tx, mut many_rx) = mpsc::channel::<(Vec<u8>, Vec<Vec<u8>>, Pid, u64)>(BUFFER_SIZE);
    let (open_bi_tx, mut open_bi_rx) = mpsc::channel::<(Connection, Pid, Pid)>(BUFFER_SIZE);
    let (open_pbi_tx, mut open_pbi_rx) = mpsc::channel::<(Connection, Pid, Pid)>(BUFFER_SIZE);
    let (finish_tx, mut finish_rx) = mpsc::channel::<StreamResource>(BUFFER_SIZE);

    tokio::spawn(async move {
        while let Some(stream) = finish_rx.recv().await {
            tokio::spawn(async move {
                match run_stream_finish(stream).await {
                    Ok(_) => (),
                    Err(e) => {
                        println!("Error finishing stream {:?}", e)
                    }
                }
            });
        }
    });

    tokio::spawn(async move {
        while let Some((connection, waiting, listener_pid)) = open_bi_rx.recv().await {
            tokio::spawn(async move {
                match run_bidirectional_open(connection, waiting, listener_pid).await {
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
        while let Some((connection, waiting, listener_pid)) = open_pbi_rx.recv().await {
            tokio::spawn(async move {
                match run_pseudo_bidirectional_open(connection, waiting, listener_pid).await {
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
                match run_bidirectional_send(msg, connection, waiting, timeout).await {
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

    let node5 = node.clone();
    tokio::spawn(async move {
        while let Some((msg, addrs, waiting, timeout)) = bidirectional_many_rx.recv().await {
            let e5 = node5.clone();
            tokio::spawn(async move {
                match run_bidirectional_many_send(e5, msg, addrs, waiting, timeout).await {
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
                match run_unidirectional_many_send(e3.clone(), msg, socketaddrs, waiting, timeout)
                    .await
                {
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
                open_bi_sender: open_bi_tx,
                open_pbi_sender: open_pbi_tx,
                finish_sender: finish_tx,
                unidirectional_sender: tx,
                unidirectional_many_sender: many_tx,
                stream_sender: stream_tx,
                bidirectional_sender: bidirectional_sender,
                bidirectional_many_sender: bidirectional_many_sender,
                new_connection_sender: connect_tx,
            }),
            make_binary(env, node.public_addr().to_string().as_bytes()),
        )
            .encode(env)
    });

    if let Some(mut new_conns) = incoming_conns {
        while let Some((connection, mut incoming_messages)) = new_conns.next().await {
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
    } else {
        // TODO have way to exit?
        let _ = stop_rx.recv().await;
    }
    

    Ok(())
}

async fn run_loop_connection(
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
            make_binary(env, conn_bytes),
            rustler::ResourceArc::new(ResponderResource { sender: tx }),
        )
            .encode(env)
    });
    if let Some(pid) = rx.recv().await {
        rx.close();

        match run_connection_stream(incoming_messages, pid).await {
            Ok(_) => (),
            Err(e) => {
                let mut msg_env = rustler::OwnedEnv::new();
                msg_env.send_and_clear(&pid, |env| {
                    (error(), make_binary(env, &e.to_string().as_bytes()[..])).encode(env)
                });
            }
        }
    }

    Ok(())
}

async fn run_connection_stream(
    incoming_messages: &mut ConnectionIncoming,
    pid: Pid,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut msg_env = rustler::OwnedEnv::new();
    while let Some((bytes, recv_stream, maybe_stream)) = incoming_messages.next_stream().await? {
        tokio::spawn(async move {
            if let Err(_error) = run_stream(bytes, pid, recv_stream, maybe_stream).await {
                // error!("Error reading stream {:?}", error);
            }
        });
    }

    msg_env.send_and_clear(&pid, |env| (connection_stopped()).encode(env));

    Ok(())
}

async fn run_stream(
    first_bytes: Bytes,
    pid: Pid,
    recv_stream: Arc<Mutex<RecvStream>>,
    maybe_stream: Option<Arc<Mutex<SendStream>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = first_bytes;
    let mut recv = recv_stream.lock().await;

    let stream_resource = maybe_stream.map(|tx| {
        rustler::ResourceArc::new(StreamResourceArc {
            inner: StreamResource { stream: tx },
        })
    });
    let mut msg_env = rustler::OwnedEnv::new();
    loop {
        msg_env.send_and_clear(&pid, |env| {
            (
                new_message(),
                make_binary(env, &bytes[..]),
                stream_resource.clone(),
            )
                .encode(env)
        });

        if let Some(new_bytes) = recv.next().await? {
            bytes = new_bytes;
        } else {
            break;
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
        send_bidirectional_many,
        send_stream_response,
        send_stream_finish,
        send_bidirectional_open,
        send_pseudo_bidirectional_open,
    ],
    load = load
);
