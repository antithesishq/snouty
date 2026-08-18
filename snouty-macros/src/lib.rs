//! `#[cached]`, the attribute behind [`AntithesisApi`]'s response cache.
//!
//! The attribute derives the cache key from the annotated method's own
//! signature — the method name and every parameter — so a parameter added
//! to a cached handler enters the key automatically, and a parameter that
//! does not implement `Serialize` fails the build instead of silently
//! aliasing entries. The one rule this cannot enforce: a cached handler
//! must take every request-shaping value as a parameter, never read one
//! from a constant or global the key cannot see.
//!
//! ```ignore
//! #[cached(value, admit = |detail: &RunDetail| detail.status.is_terminal())]
//! pub async fn get_run(&self, run_id: &str) -> Result<RunDetail> { ... }
//!
//! #[cached(stream)]
//! pub async fn get_run_logs(&self, run_id: &str, moment: Moment) -> Result<JsonStream> { ... }
//! ```
//!
//! `value` caches the `Ok` payload as one JSON object; `stream` replays a
//! cached `JsonStream` and tees a fresh one. `admit` (value mode only) gates
//! the store on the fetched payload; a stream needs no admission — it
//! commits only when read to its end. The method must be async, take
//! `&self` with a `cache` field of [`ApiCache`], and return a `Result`.
//!
//! [`AntithesisApi`]: ../snouty/api/struct.AntithesisApi.html
//! [`ApiCache`]: ../snouty/api_cache/struct.ApiCache.html

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, FnArg, ItemFn, Pat, ReturnType, Token, parse_macro_input};

enum Mode {
    Value,
    Stream,
}

struct Args {
    mode: Mode,
    admit: Option<Expr>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mode: syn::Ident = input.parse()?;
        let mode = match mode.to_string().as_str() {
            "value" => Mode::Value,
            "stream" => Mode::Stream,
            _ => return Err(syn::Error::new(mode.span(), "expected `value` or `stream`")),
        };
        let mut admit = None;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let name: syn::Ident = input.parse()?;
            if name != "admit" {
                return Err(syn::Error::new(
                    name.span(),
                    "expected `admit = <predicate>`",
                ));
            }
            input.parse::<Token![=]>()?;
            let predicate: Expr = input.parse()?;
            if matches!(mode, Mode::Stream) {
                return Err(syn::Error::new_spanned(
                    &predicate,
                    "`admit` applies to `value` mode only; a stream commits when it is read to its end",
                ));
            }
            admit = Some(predicate);
        }
        if !input.is_empty() {
            return Err(input.error("unexpected trailing tokens"));
        }
        Ok(Self { mode, admit })
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
    let ReturnType::Type(_, ret_ty) = &sig.output else {
        return Err(syn::Error::new_spanned(
            &sig,
            "a cached handler returns a Result",
        ));
    };
    let operation = sig.ident.to_string();

    // The key is built before the body runs: the body may consume the
    // parameters, and a hit must not execute the body at all.
    let key = quote! {
        let __cache_key = self.cache.key(#operation, &( #( &#params, )* ));
    };
    // The body becomes an awaited block so every exit — tail expression,
    // `?`, and `return` — funnels into `__result` and passes the store.
    let body = quote! { (async move #block).await };

    let expanded_block = match args.mode {
        Mode::Value => {
            let store = match args.admit {
                Some(admit) => quote! {
                    let __admit = #admit;
                    if let ::std::result::Result::Ok(__value) = &__result
                        && __admit(__value)
                    {
                        self.cache.store_value(&__cache_key, __value).await;
                    }
                },
                None => quote! {
                    if let ::std::result::Result::Ok(__value) = &__result {
                        self.cache.store_value(&__cache_key, __value).await;
                    }
                },
            };
            quote! {{
                #key
                if let ::std::option::Option::Some(__cached) =
                    self.cache.lookup_value(&__cache_key).await
                {
                    return ::std::result::Result::Ok(__cached);
                }
                let __result: #ret_ty = #body;
                #store
                __result
            }}
        }
        Mode::Stream => quote! {{
            #key
            if let ::std::option::Option::Some(__stream) =
                self.cache.lookup_stream(&__cache_key).await
            {
                return ::std::result::Result::Ok(__stream);
            }
            let __result: #ret_ty = #body;
            __result.map(|__stream| self.cache.store_stream(__cache_key, __stream))
        }},
    };

    Ok(quote! {
        #(#attrs)* #vis #sig #expanded_block
    })
}
