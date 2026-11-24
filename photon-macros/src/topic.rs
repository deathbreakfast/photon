//! Implementation of the [`photon::topic`] proc macro.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, punctuated::Punctuated, Attribute, Data,
    DeriveInput, Expr, Fields, Ident, Lit, LitInt, LitStr, Meta, Token,
};

/// Parses topic macro attributes: `name`, `keyed_by`, `delivery`, `shards`, `shard_by`
struct TopicAttrs {
    name: LitStr,
    keyed_by: Option<LitStr>,
    delivery: Option<LitStr>,
    shards: Option<LitInt>,
    shard_by: Option<LitStr>,
}

impl Parse for TopicAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut keyed_by = None;
        let mut delivery = None;
        let mut shards = None;
        let mut shard_by = None;

        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;

            match ident.to_string().as_str() {
                "name" => name = Some(input.parse()?),
                "keyed_by" => keyed_by = Some(input.parse()?),
                "delivery" => delivery = Some(input.parse()?),
                "shards" => shards = Some(input.parse()?),
                "shard_by" => shard_by = Some(input.parse()?),
                _ => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown attribute: {ident}"),
                    ));
                }
            }

            if !input.is_empty() {
                let _: Token![,] = input.parse()?;
            }
        }

        let name = name.ok_or_else(|| input.error("missing required attribute: name"))?;

        Ok(Self {
            name,
            keyed_by,
            delivery,
            shards,
            shard_by,
        })
    }
}

/// Implementation of the [`crate::topic`] proc-macro attribute.
pub fn topic_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as TopicAttrs);
    let mut input = parse_macro_input!(item as DeriveInput);
    let schema_json = match topic_schema_json(&mut input) {
        Ok(json) => LitStr::new(&json, proc_macro2::Span::call_site()),
        Err(err) => return err.to_compile_error().into(),
    };

    let struct_name = &input.ident;
    let topic_name_lit = &attrs.name;
    let keyed_by_lit = attrs.keyed_by.as_ref();

    let delivery_mode = attrs
        .delivery
        .as_ref()
        .map_or_else(|| "broadcast".to_string(), syn::LitStr::value);

    let is_group = delivery_mode == "group" || delivery_mode == "consumer_group";

    let (keyed_by_descriptor, topic_key_extract) = keyed_by_lit.map_or_else(
        || {
            (
                quote! { None },
                quote! { let topic_key: Option<String> = None; },
            )
        },
        |kb| {
            let key_field = kb.value();
            (
                quote! { Some(#kb) },
                quote! {
                    let topic_key = match payload_json.get(#key_field) {
                        None => {
                            return Err(photon::PhotonError::Internal(format!(
                                "keyed_by field `{0}` missing from payload for topic {1}",
                                #key_field,
                                #topic_name_lit
                            )));
                        }
                        Some(v) => {
                            if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else {
                                // Align with shard_by: stringify JSON primitives.
                                Some(v.to_string())
                            }
                        }
                    };
                },
            )
        },
    );

    let descriptor_submit = if is_group {
        let shards = attrs
            .shards
            .as_ref()
            .map_or_else(|| quote! { 32 }, |s| quote! { #s });
        let shard_by = attrs
            .shard_by
            .as_ref()
            .map_or_else(|| quote! { None }, |s| quote! { Some(#s) });
        quote! {
            photon::TopicDescriptor::group(
                #topic_name_lit,
                #shards,
                #shard_by,
                #schema_json,
            )
        }
    } else {
        quote! {
            photon::TopicDescriptor::new(
                #topic_name_lit,
                #keyed_by_descriptor,
                #schema_json,
            )
        }
    };

    let output = quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        #input

        impl #struct_name {
            /// Publish this event on an explicit [`photon::Photon`] handle (preferred).
            pub async fn publish_on(self, photon: &photon::Photon) -> photon::Result<String> {
                let payload_json = serde_json::to_value(&self)?;
                let actor_json =
                    serde_json::json!({"System": {"operation": "photon_publish"}});
                #topic_key_extract
                photon.publish(
                    #topic_name_lit,
                    topic_key.as_deref(),
                    actor_json,
                    payload_json,
                ).await
            }

            /// Publish via the process-wide Photon from [`photon::configure`].
            ///
            /// Prefer [`Self::publish_on`] with an explicit handle.
            pub async fn publish(self) -> photon::Result<String> {
                let photon = photon::default().ok_or_else(|| {
                    photon::PhotonError::Internal(
                        "Photon not configured. Call photon::configure() at startup, \
                         or use publish_on(&photon)."
                            .to_string()
                    )
                })?;
                self.publish_on(&photon).await
            }

            /// Subscribe using an explicit [`photon::Photon`] handle (preferred).
            pub async fn subscribe_on(
                photon: &photon::Photon,
                opts: photon::SubscribeOpts,
            ) -> photon::Result<std::pin::Pin<std::boxed::Box<dyn futures::Stream<Item = photon::Result<photon::Envelope<#struct_name>>> + Send>>> {
                let after_seq = if opts.mode == photon::SubscriptionMode::Durable
                    && opts.subscription_name.is_some()
                {
                    let name = opts.subscription_name.as_deref().ok_or(
                        photon::PhotonError::SubscriptionNameRequired,
                    )?;
                    photon
                        .get_checkpoint_seq(
                            name,
                            #topic_name_lit,
                            opts.topic_key_filter.as_deref(),
                        )
                        .await?
                } else {
                    None
                };
                let stream = photon.subscribe(
                    #topic_name_lit,
                    opts.topic_key_filter.as_deref(),
                    after_seq,
                );
                let mapped = futures::StreamExt::map(stream, |r: photon::Result<photon::Event>| {
                    r.and_then(|ev| {
                        let payload = serde_json::from_value(ev.payload_json.clone())
                            .map_err(|e| photon::PhotonError::PayloadError(e.to_string()))?;
                        Ok(photon::Envelope { event: ev, payload })
                    })
                });
                Ok(std::boxed::Box::pin(mapped))
            }

            /// Subscribe via the process-wide Photon from [`photon::configure`].
            ///
            /// Prefer [`Self::subscribe_on`] with an explicit handle.
            pub async fn subscribe(
                opts: photon::SubscribeOpts,
            ) -> photon::Result<std::pin::Pin<std::boxed::Box<dyn futures::Stream<Item = photon::Result<photon::Envelope<#struct_name>>> + Send>>> {
                let photon = photon::default().ok_or_else(|| {
                    photon::PhotonError::Internal(
                        "Photon not configured. Call photon::configure() at startup, \
                         or use subscribe_on(&photon, opts)."
                            .to_string()
                    )
                })?;
                Self::subscribe_on(&photon, opts).await
            }
        }

        photon::inventory::submit! {
            #descriptor_submit
        }
    };

    output.into()
}

/// Build the descriptor `schema_json` and strip `#[photon(...)]` field attributes from `input`.
///
/// Named-field structs get `{"type":"object","properties":{..}}` with one entry per field, keyed
/// by its serialized name; `#[photon(personal)]` fields carry `"x-personal": true`. Other shapes
/// get `"{}"`. Metadata only: Photon never validates payloads against it.
fn topic_schema_json(input: &mut DeriveInput) -> syn::Result<String> {
    let rename_all = input.attrs.iter().find_map(serde_rename_all_span);
    let Data::Struct(data) = &mut input.data else {
        return Ok("{}".to_owned());
    };
    let Fields::Named(fields) = &mut data.fields else {
        return Ok("{}".to_owned());
    };

    let mut properties = serde_json::Map::new();
    for field in &mut fields.named {
        let personal = take_photon_field_attrs(&mut field.attrs)?;
        let rename = field.attrs.iter().find_map(serde_field_rename);
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(&*field, "expected a named field"))?;
        if personal {
            if let (Some(span), None) = (rename_all, &rename) {
                return Err(syn::Error::new(
                    span,
                    format!(
                        "`#[photon(personal)]` on `{ident}` can't be combined with \
                         `#[serde(rename_all)]`: give the field an explicit \
                         `#[serde(rename = \"...\")]` so the marked JSON key is certain"
                    ),
                ));
            }
        }
        let name = rename.unwrap_or_else(|| ident.to_string().trim_start_matches("r#").to_owned());
        if personal
            && name
                .chars()
                .any(|c| c == '"' || c == '\\' || c.is_control())
        {
            return Err(syn::Error::new_spanned(
                ident,
                "a `#[photon(personal)]` field's serialized name can't contain quotes, \
                 backslashes or control characters",
            ));
        }
        let mut property = serde_json::Map::new();
        if personal {
            property.insert("x-personal".to_owned(), serde_json::Value::Bool(true));
        }
        properties.insert(name, serde_json::Value::Object(property));
    }

    let schema = serde_json::json!({ "type": "object", "properties": properties });
    Ok(schema.to_string())
}

/// Remove every `#[photon(...)]` attribute; return whether one of them said `personal`.
fn take_photon_field_attrs(attrs: &mut Vec<Attribute>) -> syn::Result<bool> {
    let mut personal = false;
    let mut result = Ok(());
    attrs.retain(|attr| {
        if !attr.path().is_ident("photon") {
            return true;
        }
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("personal") {
                personal = true;
                Ok(())
            } else {
                let path = &meta.path;
                let key = quote!(#path).to_string().replace(' ', "");
                Err(meta.error(format!(
                    "unknown #[photon] field attribute: `{key}`; expected `personal`"
                )))
            }
        });
        if let Err(err) = parsed {
            if result.is_ok() {
                result = Err(err);
            }
        }
        false
    });
    result.map(|()| personal)
}

/// Parse `#[serde(...)]` items; malformed serde attributes are left for serde to report.
fn serde_metas(attr: &Attribute) -> Vec<Meta> {
    if !attr.path().is_ident("serde") {
        return Vec::new();
    }
    attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .map_or_else(|_| Vec::new(), |metas| metas.into_iter().collect())
}

/// The field's `#[serde(rename = "...")]` value, if any.
fn serde_field_rename(attr: &Attribute) -> Option<String> {
    serde_metas(attr).into_iter().find_map(|meta| match meta {
        Meta::NameValue(nv) if nv.path.is_ident("rename") => match nv.value {
            Expr::Lit(expr) => match expr.lit {
                Lit::Str(lit) => Some(lit.value()),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    })
}

/// Span of a container `#[serde(rename_all ...)]`, if present.
fn serde_rename_all_span(attr: &Attribute) -> Option<proc_macro2::Span> {
    serde_metas(attr)
        .into_iter()
        .find(|meta| meta.path().is_ident("rename_all"))
        .map(|meta| syn::spanned::Spanned::span(&meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_of(item: proc_macro2::TokenStream) -> (syn::Result<String>, DeriveInput) {
        let mut input: DeriveInput = syn::parse2(item).expect("parse struct");
        let schema = topic_schema_json(&mut input);
        (schema, input)
    }

    fn schema_value(item: proc_macro2::TokenStream) -> serde_json::Value {
        let (schema, _) = schema_of(item);
        serde_json::from_str(&schema.expect("schema")).expect("json")
    }

    #[test]
    fn personal_field_is_marked_in_schema() {
        let schema = schema_value(quote! {
            pub struct Signup {
                pub user_id: String,
                #[photon(personal)]
                pub email: String,
            }
        });
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["email"]["x-personal"], true);
        assert_eq!(schema["properties"]["user_id"], serde_json::json!({}));
    }

    #[test]
    fn photon_attrs_are_stripped_and_others_kept() {
        let (schema, input) = schema_of(quote! {
            pub struct Signup {
                #[photon(personal)]
                #[serde(default)]
                pub email: String,
            }
        });
        schema.expect("schema");
        let Data::Struct(data) = &input.data else {
            panic!("struct");
        };
        let attrs = &data.fields.iter().next().expect("field").attrs;
        assert_eq!(attrs.len(), 1);
        assert!(attrs[0].path().is_ident("serde"));
    }

    #[test]
    fn serde_rename_sets_the_marked_key() {
        let schema = schema_value(quote! {
            pub struct Signup {
                #[photon(personal)]
                #[serde(rename = "emailAddress", default)]
                pub email: String,
            }
        });
        assert_eq!(schema["properties"]["emailAddress"]["x-personal"], true);
        assert!(schema["properties"].get("email").is_none());
    }

    #[test]
    fn unknown_photon_key_is_an_error_naming_it() {
        let (schema, _) = schema_of(quote! {
            pub struct Signup {
                #[photon(secret)]
                pub email: String,
            }
        });
        let message = schema.expect_err("unknown key").to_string();
        assert!(
            message.contains("unknown #[photon] field attribute: `secret`"),
            "{message}"
        );
    }

    #[test]
    fn personal_with_rename_all_needs_explicit_rename() {
        let (schema, _) = schema_of(quote! {
            #[serde(rename_all = "camelCase")]
            pub struct Signup {
                #[photon(personal)]
                pub email_address: String,
            }
        });
        let message = schema.expect_err("rename_all").to_string();
        assert!(message.contains("rename_all"), "{message}");

        let schema = schema_value(quote! {
            #[serde(rename_all = "camelCase")]
            pub struct Signup {
                #[photon(personal)]
                #[serde(rename = "emailAddress")]
                pub email_address: String,
            }
        });
        assert_eq!(schema["properties"]["emailAddress"]["x-personal"], true);
    }

    #[test]
    fn tuple_struct_keeps_empty_schema() {
        let (schema, _) = schema_of(quote! { pub struct Ping(u32); });
        assert_eq!(schema.expect("schema"), "{}");
    }
}
