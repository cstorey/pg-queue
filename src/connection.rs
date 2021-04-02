use std::{
    pin::Pin,
    task::{self, Poll},
};

use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{AsyncMessage, Connection};
use tracing::{info, warn};

pub(crate) struct NotificationStream<S, T> {
    conn: Connection<S, T>,
}

impl<S, T> NotificationStream<S, T> {
    pub(crate) fn new(conn: Connection<S, T>) -> Self {
        Self { conn }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> futures::Stream
    for NotificationStream<S, T>
{
    type Item = Result<tokio_postgres::Notification, tokio_postgres::Error>;

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
