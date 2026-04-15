use async_graphql::{Context, ErrorExtensions, Object, Result as GqlResult};
use uuid::Uuid;

use crate::AppState;
use crate::db;
use crate::error::AppError;
use crate::graphql::types::{NotificationConnection, NotificationGql};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn notifier_notifications(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 20)] first: i32,
        after: Option<Uuid>,
    ) -> GqlResult<NotificationConnection> {
        let state = ctx.data::<AppState>()?;
        let passport = ctx
            .data::<br_service_core::Passport>()
            .map_err(|_| AppError::Unauthenticated.extend())?;

        let limit = first.clamp(1, 100) as i64;

        let mut tx = state
            .pool
            .begin()
            .await
            .map_err(|e| AppError::from(e).extend())?;
        br_service_core::set_rls_context(&mut tx, passport)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to set RLS context");
                AppError::Internal.extend()
            })?;

        let rows = db::list_notifications(&mut tx, limit + 1, after)
            .await
            .map_err(|e| AppError::from(e).extend())?;

        tx.commit()
            .await
            .map_err(|e| AppError::from(e).extend())?;

        let has_next_page = rows.len() as i64 > limit;
        let nodes: Vec<NotificationGql> = rows
            .into_iter()
            .take(limit as usize)
            .map(NotificationGql::from)
            .collect();

        Ok(NotificationConnection {
            nodes,
            has_next_page,
        })
    }

    async fn notifier_unread_count(&self, ctx: &Context<'_>) -> GqlResult<i64> {
        let state = ctx.data::<AppState>()?;
        let passport = ctx
            .data::<br_service_core::Passport>()
            .map_err(|_| AppError::Unauthenticated.extend())?;

        let mut tx = state
            .pool
            .begin()
            .await
            .map_err(|e| AppError::from(e).extend())?;
        br_service_core::set_rls_context(&mut tx, passport)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to set RLS context");
                AppError::Internal.extend()
            })?;

        let count = db::unread_count(&mut tx)
            .await
            .map_err(|e| AppError::from(e).extend())?;

        tx.commit()
            .await
            .map_err(|e| AppError::from(e).extend())?;

        Ok(count)
    }
}
