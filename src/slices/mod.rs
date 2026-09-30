service_engine::compose_service! {
    principal = crate::kernel::AppPrincipal;
    prefix = notifier;
    slice notifications ["notifications"] {
        query = notifications::graphql::NotificationsQuery,
        mutation = notifications::graphql::NotificationsMutation,
        subscription = notifications::graphql::NotificationsSubscription
    }
}
