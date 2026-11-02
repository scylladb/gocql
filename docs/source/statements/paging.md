# Paging and borrowed results

`query.Run(ctx, parameters)` advances pages through `Rows.Next`/`NextInto`; `Row()` returns the current
typed value. `NextInto` writes fields into a caller struct after the page has
arrived. Payload fields still borrow the receive page. Their lifetime ends on
the next advancement or Close unless a retained `PageLease` pins that page.
Clone explicitly detaches a value. A retained page survives Session.Close and
does not retain query input storage.

For explicit paging use `query.RunPage(ctx, parameters, StartPage())` and
carry the returned page state to the next call. `query.RunUnpaged` explicitly requests
one unpaged response. Check iteration errors and close all delivered pages.
Cyclic/nonprogressing state and retained paging-history limits report errors.

Use `RowInto` with `OwnedBlobCodec` for reusable owned buffered destinations.
See [streaming](streaming.md) for seeded streaming decoders and explicit
incremental receive before a complete page arrives.
