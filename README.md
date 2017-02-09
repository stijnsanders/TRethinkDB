# Delphi RethinkDB driver

A Delphi driver for [RethinkDB](https://rethinkdb.com/)

Add these units to your project:

* [jsonDoc](https://github.com/stijnsanders/jsonDoc#jsonDoc).pas
* simpleSock.pas
* ProtBuf.pas
* ql2.pas
* RethinkDB.pas
* RethinkDBAuth.pas

From the `RethinkDB` unit use the `TRethinkDBConnection` object to open a connection to a RethinkDB server.

By default there is no `r` variable declared like with drivers for other languages, but if you want to, you can include this in your project: `type r=TRethinkDB;`

**WARNING:** The current version only works blocking, not asynchronous, and is not thread-safe.

See also

* [Ten minute guide to RethinkDB](https://rethinkdb.com/docs/guide/javascript/)
* [DelphiProtocolBuffer](https://github.com/stijnsanders/DelphiProtocolBuffer#delphiprotocolbuffer) for converting RethinkDB's `ql2.proto` into `ql2.pas`
* [jsonDoc](https://github.com/stijnsanders/jsonDoc#jsonDoc) repository
* `simpleSock.pas` re-used from [xxm](https://github.com/stijnsanders/xxm/blob/master/Delphi/http/xxmSock.pas) and [TMongoWire](https://github.com/stijnsanders/TMongoWire/blob/master/simpleSock.pas)
* [yoy.be/md5](http://yoy.be/md5.html) for SCRAM SHA256 HMAC PBKDF2
