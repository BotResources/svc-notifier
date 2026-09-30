use futures_util::future::BoxFuture;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use super::aggregate::{Cause, Notification, NotificationRow};
use super::store;
use crate::kernel::{AppFault, AppPrincipal};

// A foreign or unknown id is ignored: the acknowledgement stays a success and no
// delta leaves, so a caller cannot tell "not yours" from "not there".

pub struct MarkAsRead {
    pub id: Uuid,
}

impl MutationInput for MarkAsRead {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "mark_as_read";
}

pub fn mark_as_read<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: MarkAsRead,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move { mark_read(cx, &[input.id]).await })
}

pub struct MarkAllAsRead;

impl MutationInput for MarkAllAsRead {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "mark_all_as_read";
}

pub fn mark_all_as_read<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    _input: MarkAllAsRead,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let me = cx.principal().recipient();
        let unread = store::unread_ids_of_recipient(cx.connection(), me).await?;
        mark_read(cx, &unread).await
    })
}

async fn mark_read(cx: &mut Mutation<'_, AppPrincipal>, ids: &[Uuid]) -> Result<(), AppFault> {
    let me = cx.principal().recipient();
    let now = cx.now().as_datetime();
    for mut notification in owned(cx, ids, me).await? {
        if notification.mark_read(now) {
            cx.save(&notification).await?;
            cx.impact_caused::<Notification, _>(&notification.id, Cause::Read)?;
        }
    }
    Ok(())
}

pub struct DeleteNotifications {
    pub ids: Vec<Uuid>,
}

impl MutationInput for DeleteNotifications {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "delete_notifications";
}

pub fn delete_notifications<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: DeleteNotifications,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let me = cx.principal().recipient();
        for notification in owned(cx, &input.ids, me).await? {
            cx.delete(&notification).await?;
            cx.impact_caused::<Notification, _>(&notification.id, Cause::Deleted)?;
        }
        Ok(())
    })
}

async fn owned(
    cx: &mut Mutation<'_, AppPrincipal>,
    ids: &[Uuid],
    recipient: Uuid,
) -> Result<Vec<NotificationRow>, AppFault> {
    let loaded = cx.load_many::<NotificationRow>(ids).await?;
    Ok(loaded
        .into_iter()
        .filter(|notification| notification.belongs_to(recipient))
        .collect())
}
