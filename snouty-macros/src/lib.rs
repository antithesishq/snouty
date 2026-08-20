//! `#[cached]`, the attribute behind `AntithesisApi`'s response cache.
//!
//! The attribute derives the cache key from the annotated method's own
//! signature — the method name and every parameter — so a parameter added
//! to a cached handler enters the key automatically, and a parameter that
//! does not implement `Serialize` fails the build instead of silently
//! aliasing entries. The one rule this cannot enforce: a cached handler
//! must take every request-shaping value as a parameter, never read one
//! from a constant or global the key cannot see.
//!
//! The cache stores nothing untagged: a cached handler returns
//! `Result<Tagged<T, CachePolicy>>` — the fetched value plus the handler's
//! own admission verdict, which the caller untags. Admission therefore
//! lives in the handler, next to the response, whose cache headers carry
//! the other half of the verdict (`ApiCache::headers_admit`).
//!
//! ```ignore
//! #[cached(value)]
//! pub async fn get_run(&self, run_id: &str) -> Result<Tagged<RunDetail, CachePolicy>> {
//!     // ...fetch, then tag:
//!     Ok(detail.with_tag(cache_policy))
//! }
//! ```
//!
//! (The example is `ignore` because the expansion calls the `ApiCache` on
//! the receiver's `cache` field; a compiling doctest would need a stub of
//! snouty's cache. The real handlers in `api.rs` are the tested example.)
//!
//! `value` caches the `Ok` payload as one JSON object; `stream` replays a
//! cached tagged `JsonStream` and tees a fresh one, committing only when it
//! is read to its end. The method must be async and take `&self` with a
//! `cache` field of `ApiCache`.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{FnArg, ItemFn, Pat, ReturnType, parse_macro_input};

enum Mode {
    Value,
    Stream,
}

struct Args {
    mode: Mode,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mode: syn::Ident = input.parse()?;
        let mode = match mode.to_string().as_str() {
            "value" => Mode::Value,
            "stream" => Mode::Stream,
            _ => return Err(syn::Error::new(mode.span(), "expected `value` or `stream`")),
        };
        if !input.is_empty() {
            return Err(input.error("unexpected trailing tokens"));
        }
        Ok(Self { mode })
    }
}

#[proc_macro_attribute]
pub fn cached(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as Args);
    let func = parse_macro_input!(item as ItemFn);
    match expand(args, func) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand(args: Args, func: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = func;
    if sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            sig.fn_token,
            "a cached handler must be async",
        ));
    }
    let mut has_receiver = false;
    let mut params = Vec::new();
    for arg in &sig.inputs {
        match arg {
            FnArg::Receiver(_) => has_receiver = true,
            FnArg::Typed(arg) => match arg.pat.as_ref() {
                Pat::Ident(pat) => params.push(pat.ident.clone()),
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "a cached handler's parameters must be plain identifiers, \
                         so every one reaches the cache key",
                    ));
                }
            },
        }
    }
    if !has_receiver {
        return Err(syn::Error::new_spanned(
            &sig.inputs,
            "a cached handler takes &self; the cache lives on it",
        ));
    }
    let tagged_return = match &sig.output {
        ReturnType::Type(_, ty) => quote!(#ty).to_string().contains("Tagged"),
        ReturnType::Default => false,
    };
    if !tagged_return {
        return Err(syn::Error::new_spanned(
            &sig.output,
            "a cached handler returns Result<Tagged<T, CachePolicy>>: \
             the cache stores nothing untagged",
        ));
    }
    let operation = sig.ident.to_string();

    // The key is built before the body runs: the body may consume the
    // parameters, and a hit must not execute the body at all.
    let key = quote! {
        let __cache_key = self.cache.key(#operation, &( #( &#params, )* ));
    };
    // The body becomes an awaited block so every exit — tail expression,
    // `?`, and `return` — funnels into one `Result<Tagged<T>>` that passes
    // the store.
    let body = quote! { (async move #block).await };

    let lookup = match args.mode {
        Mode::Value => quote! { self.cache.lookup_value(&__cache_key).await },
        Mode::Stream => quote! { self.cache.lookup_stream(&__cache_key).await },
    };
    // The cache itself honors the tag: store_value stores only a Cacheable
    // value, and store_stream tees only a Cacheable stream.
    let store = match args.mode {
        Mode::Value => quote! {
            self.cache.store_value(&__cache_key, &__tagged).await;
            ::std::result::Result::Ok(__tagged)
        },
        Mode::Stream => quote! {
            ::std::result::Result::Ok(self.cache.store_stream(__cache_key, __tagged))
        },
    };
    let expanded_block = quote! {{
        #key
        if let ::std::option::Option::Some(__cached) = #lookup {
            return ::std::result::Result::Ok(__cached);
        }
        match #body {
            ::std::result::Result::Ok(__tagged) => { #store }
            ::std::result::Result::Err(__err) => ::std::result::Result::Err(__err),
        }
    }};

    Ok(quote! {
        #(#attrs)* #vis #sig #expanded_block
    })
}
