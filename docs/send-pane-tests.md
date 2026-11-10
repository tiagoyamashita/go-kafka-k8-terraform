# Send pane tests

The left pane is the composer. It posts the textarea to `transfers.in` and swaps only `#composer`. A green line means Kafka accepted the body. It does not mean the receiver stored it or the transmitter published it.

Run these against http://127.0.0.1:8088 with the receiver and transmitter up, unless a row says to stop one of them. After each send, look at the left pane first, then the history on the right.

## Accepted

| # | Paste into the box | Left pane | Also check |
| --- | --- | --- | --- |
| 1 | The sample already in the box | Green "Sent" line with that id. The box is replaced by a new sample. | The same id appears on the right as `received`, then `published`. |
| 2 | Same JSON as a sent message, with a new UUID | Green line for the new id. The previous text is gone from the box. | One new history row. The old row is unchanged. |
| 3 | `{"id":"<uuid>","type":"transfer","payload":{"amount":10,"note":"a b"}}` | Green line. | Stored payload still contains `note`. |
| 4 | Leading and trailing spaces or newlines around a valid object | Green line. | Kafka value is the trimmed text. |
| 5 | `{"id":"<uuid>","payload":"not-an-object"}` | Green line. The top-level value is an object, so `payload` may be a string. | History stores that body unchanged. |
| 6 | Two sends in a row, double-click Send on the second | One green line. The box shows a fresh id, not the one just sent. | Two history rows, not three. |

## Rejected in the pane

The box keeps what you typed. Nothing is produced. The right-hand list does not gain a row.

| # | Paste into the box | Left pane |
| --- | --- | --- |
| 7 | Empty box | "The message must be a JSON object with a UUID id." |
| 8 | `   ` | Same id error. |
| 9 | `not json` | Same id error. |
| 10 | `{"id":"<uuid>"` | Same id error. |
| 11 | `[{"id":"<uuid>"}]` | Same id error. A JSON array is not an object. |
| 12 | `{"type":"transfer","payload":{"amount":10}}` | Same id error. |
| 13 | `{"id":"","type":"transfer"}` | Same id error. |
| 14 | `{"id":"transfer-1","type":"transfer"}` | Same id error. |
| 15 | `{"id":"11111111-1111-1111-1111-11111111111","type":"transfer"}` | Same id error. The id is one character short. |
| 16 | `{"id": 1}` | Same id error. |
| 17 | `"just a string"` | Same id error. |
| 18 | A valid object larger than 64KiB | "The message is larger than 64KiB." The box still shows the text. |

## Accepted by the pane, then the use case fails

These are the ones that look successful on the left and still break the transfer.

| # | Setup | Left pane | Failure |
| --- | --- | --- | --- |
| 19 | Stop the receiver. Send the sample. | Green "Sent" line. | The right pane stays empty. Kafka has the record and nothing has stored it. |
| 20 | Start the receiver again after test 19. | No new line until you send again. | The receiver should pick up test 19 without another click. If the row never appears, the offset was not available to this group. |
| 21 | Send the same id a second time after it is already stored. | Green line again. The pane treats this as a new produce. | The right pane still has one row for that id. The second copy is in Kafka and the insert is skipped. |
| 22 | Stop Kafka, or point the console at a closed broker. Send the sample. | "Kafka did not accept the message: …" The typed JSON stays in the box. | No history row. |
| 23 | Receiver up, transmitter stopped. Send a new id. | Green line. | The row stays `received`. `published` never arrives. |
| 24 | Send `{"id":"<uuid>","type":"transfer","payload":{"amount":"ten"}}` | Green line. | The pipeline stores it. Nothing checks that `amount` is a number. |
| 25 | Send a valid object whose `id` differs from a nested `payload.id` | Green line uses the top-level id. | History is keyed by the top-level id only. |

## Pane behavior

| # | Action | Expected |
| --- | --- | --- |
| 26 | Send a valid message, then press Send again without editing | The second send uses the new sample, not the message that just succeeded. |
| 27 | Send an invalid body, fix one character, send again | The error goes away only after a later send succeeds or fails with a new message. A failed send must not clear the textarea. |
| 28 | Put `<script>alert(1)</script>` inside a valid object and send an invalid body first so the text is re-rendered | The box shows the script as text. The page does not run it. |
| 29 | Reload the page | The box is a new sample. The green line and any error are gone. History on the right remains. |
