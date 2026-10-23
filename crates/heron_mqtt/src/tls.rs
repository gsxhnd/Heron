//! TLS 1.2 and TLS 1.3 acceptors for MQTTS and WSS.

use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ServerConfig, WebPkiClientVerifier};
use tokio_rustls::TlsAcceptor;

/// Whether MQTTS/WSS clients must present a certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCertMode {
    /// Do not request a client certificate.
    Disabled,
    /// Request a certificate and accept connections that omit one.
    Optional,
    /// Require a certificate chaining to `client_ca_pem`.
    Required,
}

/// Build a `rustls` acceptor that offers TLS 1.3 and TLS 1.2.
pub fn acceptor_from_pem(
    cert_pem: &[u8],
    key_pem: &[u8],
    client_ca_pem: Option<&[u8]>,
    client_auth: ClientCertMode,
) -> Result<TlsAcceptor, Error> {
    let certs = load_certs(cert_pem)?;
    let key = load_key(key_pem)?;
    let builder =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(tls_error)?;
    let config = match client_auth {
        ClientCertMode::Disabled => builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(tls_error)?,
        ClientCertMode::Optional | ClientCertMode::Required => {
            let ca_pem = client_ca_pem.ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "client_ca_pem is required when client certificates are enabled",
                )
            })?;
            let roots = load_roots(ca_pem)?;
            let mut verifier = WebPkiClientVerifier::builder(Arc::new(roots));
            if client_auth == ClientCertMode::Optional {
                verifier = verifier.allow_unauthenticated();
            }
            let verifier = verifier.build().map_err(tls_error)?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(tls_error)?
        }
    };
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// DNS SAN when one is present, otherwise the subject common name.
pub fn certificate_identity(der: &[u8]) -> Option<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).ok()?;
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::DNSName(dns) = name {
                return Some((*dns).to_owned());
            }
        }
    }
    cert.subject()
        .iter_common_name()
        .next()
        .and_then(|name| name.as_str().ok())
        .map(str::to_owned)
}

fn load_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, Error> {
    rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))
}

fn load_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, Error> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "TLS private key is missing"))
}

fn load_roots(pem: &[u8]) -> Result<RootCertStore, Error> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(pem)? {
        roots.add(cert).map_err(tls_error)?;
    }
    if roots.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "client CA is empty"));
    }
    Ok(roots)
}

fn tls_error(err: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::InvalidInput, err.to_string())
}
