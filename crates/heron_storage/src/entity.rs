//! SeaORM models for the broker tables.

use sea_orm::entity::prelude::*;

/// `clients`
pub mod client {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "clients")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = true)]
        pub id: i64,
        #[sea_orm(unique)]
        pub client_id: String,
        pub protocol_version: i16,
        pub ip_address: Option<String>,
        pub port: Option<i32>,
        pub connected: bool,
        pub connected_at: Option<DateTime>,
        pub disconnected_at: Option<DateTime>,
        pub keep_alive: Option<i32>,
        pub clean_session: Option<bool>,
        pub will_topic: Option<String>,
        pub will_qos: Option<i16>,
        pub will_retain: Option<bool>,
        pub will_payload: Option<Vec<u8>>,
        pub will_delay: Option<i32>,
        pub created_at: DateTime,
        pub updated_at: DateTime,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// `sessions`
pub mod session {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "sessions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = true)]
        pub id: i64,
        #[sea_orm(unique)]
        pub client_id: String,
        pub clean_session: bool,
        pub session_expiry_interval: Option<i32>,
        pub created_at: DateTime,
        pub updated_at: DateTime,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// `subscriptions`
pub mod subscription {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "subscriptions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = true)]
        pub id: i64,
        pub client_id: String,
        pub topic: String,
        pub qos: i16,
        pub no_local: bool,
        pub retain_as_published: bool,
        pub retain_handling: i16,
        pub created_at: DateTime,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// `retained_messages`
pub mod retained {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "retained_messages")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = true)]
        pub id: i64,
        #[sea_orm(unique)]
        pub topic: String,
        pub qos: i16,
        pub payload: Option<Vec<u8>>,
        pub payload_size: Option<i32>,
        pub created_at: DateTime,
        pub updated_at: DateTime,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// `messages`
pub mod message {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "messages")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = true)]
        pub id: i64,
        pub client_id: String,
        pub packet_id: i32,
        pub topic: String,
        pub qos: i16,
        pub payload: Option<Vec<u8>>,
        pub payload_size: Option<i32>,
        pub retain: bool,
        pub dup: bool,
        pub direction: String,
        pub state: String,
        pub created_at: DateTime,
        pub updated_at: DateTime,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
