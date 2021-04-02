use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use futures::{ready, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
};
use tokio_postgres::{AsyncMessage, Connection};
use tracing::{debug, info, warn};

use crate::logs::Result;

pub(crate) struct NotificationStream<S, T> {
    conn: Connection<S, T>,
}

pub(crate) async fn notify_on_notification<
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
>(
    conn: Connection<S, T>,
    notify: Arc<Notify>,
) -> Result<()> {
    debug!("Listening for notifies on connection");

    let mut notifies = NotificationStream::new(conn);

    while let Some(notification) = notifies.next().await.transpose()? {
        debug!(?notification, "Received notification");
        notify.notify_waiters();
    }

    Ok(())
}

impl<S, T> NotificationStream<S, T> {
    pub(crate) fn new(conn: Connection<S, T>) -> Self {
        Self { conn }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> futures::Stream
    for NotificationStream<S, T>
{
    type Item = std::result::Result<tokio_postgres::Notification, tokio_postgres::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let maybe_message = ready!(self.conn.poll_message(cx));
        let message = match maybe_message {
            Some(Ok(item)) => item,
            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            None => return Poll::Ready(None),
        };

        match message {
            AsyncMessage::Notice(notice) => {
                info!(?notice, "Db notice");
                Poll::Pending
            }
            AsyncMessage::Notification(notification) => Poll::Ready(Some(Ok(notification))),
            message => {
                warn!(?message, "Other message received");
                Poll::Pending
            }
        }
    }
}
