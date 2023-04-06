//! HTTP Write V1 and V2 implementation logic for both single, and multi-tenant
//! operational modes.

pub mod v1;
pub mod v2;

pub mod multi_tenant;
pub mod single_tenant;

pub mod auth;

mod params;
pub use params::*;
