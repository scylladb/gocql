# Paging and borrowed results

`Run` advances pages through `Rows.Next`/`NextInto`; `Row()` returns the current
typed value. `NextInto` writes fields into a caller struct after the page has
arrived. Payload fields still borrow the receive page. Their lifetime ends on
the next advancement or Close unless a retained `PageLease` pins that page.
Clone explicitly detaches a value. A retained page survives Session.Close and
does not extend a borrowed input binding's drain.

For explicit paging use `RunPage(ctx, session, bound, StartPage(), options)` and
carry the returned page state to the next call. `RunUnpaged` explicitly requests
one unpaged response. Check iteration errors and close all delivered pages.
Cyclic/nonprogressing state and retained paging-history limits report errors.

See [streaming](streaming.md) to decode before a complete page arrives.
