unit RethinkDB;

{$D-}
{$L-}

interface

uses SysUtils, simpleSock, ql2, jsonDoc;

type
  TRethinkDB=class;//forward
  IRethinkDBTerm=interface;//forward
  IRethinkDB=interface;//forward
  IRethinkDBTable=interface;//forward

  //////////////////////////////////////////////////////
  //// By default no "r" is declared, but if you want,
  //// include this in your project:
  //
  //r=TRethinkDB;

  TRethinkDB=class(TObject)
  protected
    class function x(const s:WideString):IRethinkDBTerm; overload;
    class function x(b:boolean):IRethinkDBTerm; overload;
    class function x(v:integer):IRethinkDBTerm; overload;
  public
    class function db(const DBName:WideString;
      const Options:IJSONDocument=nil):IRethinkDB;
    class function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    class function dbCreate(const DBName:WideString):IRethinkDBTerm;
    class function dbDrop(const DBName:WideString):IRethinkDBTerm;
    class function dbList:IRethinkDBTerm;
  end;

  TRethinkDBConnection=class(TObject)
  private
    FSock:TTcpSocket;
    FData:UTF8String;
    FDataSize,FDataIndex:cardinal;
    FToken:int64;
    function IsConnected:boolean;
    procedure Fail(const x:string);
    function AuthEx(const d:IJSONDocument):IJSONDocument;
    procedure SendTerm(const t:IRethinkDBTerm);
    procedure Build(const s:UTF8String);
    function ReadDoc(const dd: IJSONDocument): TResponseType;
  public
    procedure AfterConstruction; override;
    destructor Destroy; override;
    procedure Connect(const Host,UserName,Password:WideString;
      Port:cardinal=28015);
    //TODO: reconnect
    procedure Close;
    //TODO: use
    function ServerInfo: IJSONDocument;

    property Connected:boolean read IsConnected;
  end;

  IRethinkDBResultSet=interface(IUnknown)
    function Get(const d:IJSONDocument):boolean;
  end;

  TRethinkDBBuilder=procedure(const x:UTF8String) of object;

  IRethinkDBTerm=interface(IUnknown)
    function Execute(Connection:TRethinkDBConnection;
      const Options:IJSONDocument=nil):IJSONDocument;
    function Run(Connection:TRethinkDBConnection;
      const Options:IJSONDocument=nil):IRethinkDBResultSet;
    //TODO: changes

    //all IRethinkDBTerm single use! //TODO: replace with central chaining
    function Chain(Next:IRethinkDBTerm):IRethinkDBTerm;
    function Next:IRethinkDBTerm;
    procedure Build(b:TRethinkDBBuilder);
  end;

  IRethinkDB=interface(IRethinkDBTerm)
    function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    function tableCreate(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTerm;
    function tableDrop(const TableName:WideString):IRethinkDBTerm;
    function tableList:IRethinkDBTerm;
  end;

  IRethinkDBTable=interface(IRethinkDBTerm)
    //TODO: indexCreate
    function indexDrop(const IndexName:WideString):IRethinkDBTerm;
    function indexList:IRethinkDBTerm;
    function indexRename(const OldName,NewName:WideString):IRethinkDBTerm;

    function get(const Key:WideString):IRethinkDBTerm;
    function getAll:IRethinkDBTerm;
  end;

  TRethinkDBTerm=class(TTHREADUNSAFEInterfacedObject,IRethinkDBTerm)//abstract
  private
    FNext:IRethinkDBTerm;
  protected
    function Execute(Connection:TRethinkDBConnection;
      const Options:IJSONDocument=nil):IJSONDocument;
    function Run(Connection:TRethinkDBConnection;
      const Options:IJSONDocument=nil):IRethinkDBResultSet;

    function Chain(Next:IRethinkDBTerm):IRethinkDBTerm;
    function Next:IRethinkDBTerm;
    procedure Build(b:TRethinkDBBuilder); virtual; abstract;
  public
    constructor Create;
  end;

  TRethinkDBValue=class(TRethinkDBTerm,IRethinkDBTerm)
  private
    FTermType:TTermType;
    FFirstArg:IRethinkDBTerm;
    FOptions:IJSONDocument;
  protected
    procedure Build(b:TRethinkDBBuilder); override;
  public
    constructor Create(tt:TTermType;const arg:IRethinkDBTerm=nil;
      const opt:IJSONDocument=nil);
  end;

  TRethinkDBConstant=class(TRethinkDBTerm,IRethinkDBTerm)
  private
    FData:UTF8String;
  protected
    procedure Build(b:TRethinkDBBuilder); override;
  public
    constructor Create(const Literal:UTF8String);
  end;

  TRethinkDB_DB=class(TRethinkDBValue,IRethinkDB)
  protected
    function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    function tableCreate(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTerm;
    function tableDrop(const TableName:WideString):IRethinkDBTerm;
    function tableList:IRethinkDBTerm;
  end;

  TRethinkDBTable=class(TRethinkDBValue,IRethinkDBTable)
  protected
    function indexDrop(const IndexName:WideString):IRethinkDBTerm;
    function indexList:IRethinkDBTerm;
    function indexRename(const OldName,NewName:WideString):IRethinkDBTerm;

    function get(const Key:WideString):IRethinkDBTerm;
    function getAll:IRethinkDBTerm;
  end;

  TRethinkDBResultSet=class(TTHREADUNSAFEInterfacedObject,IRethinkDBResultSet)
  public
    function Get(const d:IJSONDocument):boolean;
  end;

  ERethinkDBError=class(Exception);
  ERethinkDBErrorCode=class(ERethinkDBError)
  private
    FCode:integer;
  public
    constructor Create(const Msg:string;Code:integer);
    constructor CreateFromDoc(const d:IJSONDocument);
    property Code:integer read FCode;
  end;
  ERethinkDBClientError=class(ERethinkDBErrorCode);
  ERethinkDBCompileError=class(ERethinkDBErrorCode);
  ERethinkDBRuntimeError=class(ERethinkDBErrorCode);

  
{$IF not Declared(UTF8ToWideString)}
{$DEFINE NOT_DECLARED_UTF8ToWideString}
{$IFEND}

{$IFDEF NOT_DECLARED_UTF8ToWideString}
function UTF8ToWideString(const s: UTF8String): WideString;
{$ENDIF}

implementation

uses Variants, RethinkDBAuth;

{$IFDEF NOT_DECLARED_UTF8ToWideString}
function UTF8ToWideString(const s: UTF8String): WideString;
begin
  Result:=UTF8Decode(s);
end;
{$ENDIF}

{ TRethinkDB }

class function TRethinkDB.x(const s:WideString): IRethinkDBTerm;
var
  s1,s2:UTF8String;
  i1,i2,l1,l2:integer;
begin
  //Result:=TRethinkDBConstant.Create('"'+StringReplace(s,'"','\"',[rfReplaceAll])+'"');
  s1:=UTF8Encode(s);
  l1:=Length(s1);
  l2:=l1+2;
  i1:=0;
  i2:=1;
  SetLength(s2,l2);
  s2[1]:='"';
  while i1<l1 do
   begin
    inc(i1);
    if s1[i1]='"' then //more?
     begin
      if i2=l2 then
       begin
        inc(l2,$100);
        SetLength(s2,l2);
       end;
      inc(i2);
      s2[i2]:='\';
     end;
    if i2=l2 then
     begin
      inc(l2,$100);
      SetLength(s2,l2);
     end;
    inc(i2);
    s2[i2]:=s1[i1];
   end;
  if i2=l2 then
   begin
    inc(l2,$100);
    SetLength(s2,l2);
   end;
  inc(i2);
  s2[i2]:='"';
  SetLength(s2,i2);
  Result:=TRethinkDBConstant.Create(s2);
end;

class function TRethinkDB.x(b: boolean): IRethinkDBTerm;
begin
  if b then
    Result:=TRethinkDBConstant.Create('true')
  else
    Result:=TRethinkDBConstant.Create('false');
end;

class function TRethinkDB.x(v: integer): IRethinkDBTerm;
begin
  Result:=TRethinkDBConstant.Create(IntToStr(v));
end;

class function TRethinkDB.db(const DBName: WideString;
  const Options: IJSONDocument): IRethinkDB;
begin
  Result:=TRethinkDB_DB.Create(TermType_DB,x(DBName),Options);
end;

class function TRethinkDB.table(const TableName: WideString;
  const Options: IJSONDocument): IRethinkDBTable;
begin
  Result:=TRethinkDBTable.Create(TermType_TABLE,x(TableName),Options);
end;

class function TRethinkDB.dbCreate(
  const DBName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DB_CREATE,x(DBName));
end;

class function TRethinkDB.dbDrop(const DBName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DB_DROP,x(DBName));
end;

class function TRethinkDB.dbList: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DB_LIST);
end;

{ r }

type
  r=TRethinkDB;

{ TRethinkDBTerm }

constructor TRethinkDBTerm.Create;
begin
  inherited Create;
  FNext:=nil;
end;

function TRethinkDBTerm.Chain(Next: IRethinkDBTerm):IRethinkDBTerm;
begin
  //TODO: debug only?
  if FNext<>nil then
    raise ERethinkDBError.Create('Duplicate chain detected. IRethinkDBTerms are single use only!');
  FNext:=Next;
  Result:=Self;
end;

function TRethinkDBTerm.Next: IRethinkDBTerm;
begin
  Result:=FNext;
end;

function TRethinkDBTerm.Execute(Connection: TRethinkDBConnection; const Options: IJSONDocument): IJSONDocument;
begin
  Connection.SendTerm(Self);
  Result:=JSON;
  Connection.ReadDoc(Result);
end;

function TRethinkDBTerm.Run(Connection: TRethinkDBConnection;
  const Options: IJSONDocument): IRethinkDBResultSet;
begin
  Connection.SendTerm(Self);
  TRethinkDBResultSet.Create;//();
  //////////
  Result:=nil;
end;

{ TRethinkDBValue }

constructor TRethinkDBValue.Create(tt:TTermType;const arg:IRethinkDBTerm=nil;
  const opt:IJSONDocument=nil);
begin
  inherited Create;
  FTermType:=tt;
  FFirstArg:=arg;
  FOptions:=opt;
end;

procedure TRethinkDBValue.Build(b: TRethinkDBBuilder);
var
  v:IRethinkDBTerm;
begin
  b('[');
  b(IntToStr(integer(FTermType)));
  if FFirstArg=nil then
   begin
    b(',[]');//?
   end
  else
   begin
    b(',[');
    v:=FFirstArg;
    while v<>nil do
     begin
      v.Build(b);
      v:=v.Next;
      if v=nil then b(']') else b(',');
     end;
   end;
  if FOptions<>nil then
   begin
    //e:=JSONEnum(FOptions);//TODO
    b(FOptions.ToString);
   end;
  b(']');
end;

{ TRethinkDBConstant }

constructor TRethinkDBConstant.Create(const Literal: UTF8String);
begin
  inherited Create;
  FData:=Literal;
end;

procedure TRethinkDBConstant.Build(b: TRethinkDBBuilder);
begin
  b(FData);
end;

{ TRethinkDBConnection }

const
  RethinkDBConnection_Data_GrowStep=$1000;

procedure TRethinkDBConnection.AfterConstruction;
begin
  inherited;
  FSock:=nil;
  FToken:=100000;//random?
  FDataSize:=RethinkDBConnection_Data_GrowStep;
  SetLength(FData,RethinkDBConnection_Data_GrowStep);
end;

destructor TRethinkDBConnection.Destroy;
begin
  FreeAndNil(FSock);
  inherited;
end;

procedure TRethinkDBConnection.Connect(const Host,UserName,Password:WideString;Port:cardinal=28015);
var
  l:cardinal;
  s:UTF8String;
begin
  FreeAndNil(FSock);
  FSock:=TTcpSocket.Create(AF_INET);//TODO: switch for AF_INET6
  FSock.Connect(Host,Port);

  l:=cardinal(Version_V1_0);
  FSock.SendBuf(l,4);

  l:=0;
  while (l=0) or (s[l]<>#0) do
   begin
    SetLength(s,l+$10000);
    inc(l,FSock.ReceiveBuf(s[l+1],$10000));
   end;
  if l=0 then s:='' else SetLength(s,l-1);
  //:=JSON.Parse(s)?

  //check d['success']=true?
  //min_protocol_version? max_protocol_version?
  //TODO: store d['server_version']?

  RethinkDBAuthenticate(AuthEx,0,UserName,Password);
  //TODO: move send Version_V1_0 into first send of RethinkDBAuthenticate
end;

function TRethinkDBConnection.AuthEx(const d:IJSONDocument):IJSONDocument;
var
  s:UTF8String;
  l:cardinal;
begin
  s:=UTF8Encode(d.ToString)+#0;
  FSock.SendBuf(s[1],Length(s));
  l:=0;
  while (l=0) or (s[l]<>#0) do
   begin
    SetLength(s,l+$10000);
    inc(l,FSock.ReceiveBuf(s[l+1],$10000));
   end;
  if l=0 then s:='' else SetLength(s,l-1);
  try
    Result:=JSON.Parse(UTF8ToWideString(s));
  except
    on EJSONDecodeException do raise ERethinkDBError.Create(s);
  end;
  if Result['success']<>true then
    try
      raise ERethinkDBErrorCode.Create(Result['error'],Result['error_code']);
    except
      on e:Exception do
        if e is ERethinkDBErrorCode then
          raise
        else
          raise ERethinkDBError.Create(s);
    end;
end;

procedure TRethinkDBConnection.Close;
begin
  FreeAndNil(FSock);
end;

function TRethinkDBConnection.IsConnected: boolean;
begin
  Result:=(FSock<>nil) and FSock.Connected;
end;

procedure TRethinkDBConnection.Fail(const x: string);
begin
  try
    raise ERethinkDBError.Create(x);
  finally
    try
      FSock.Disconnect;
    except
      //silent!
    end;
  end;
end;

procedure TRethinkDBConnection.Build(const s: UTF8String);
var
  i,l:cardinal;
begin
  l:=Length(s);
  if l<>0 then
   begin
    i:=FDataIndex+l;
    if i>FDataSize then
     begin
      while i>FDataSize do inc(FDataSize,RethinkDBConnection_Data_GrowStep);
      SetLength(FData,FDataSize);
     end;
    Move(s[1],FData[FDataIndex+1],l);
    inc(FDataIndex,l);
   end;
end;

procedure TRethinkDBConnection.SendTerm(const t: IRethinkDBTerm);
begin
  //TODO: lock?

  FDataIndex:=12;
  Build('[1,');//QueryType_START
  t.Build(Build);
  Build(',{}]');

  inc(FToken);
  pint64(@FData[1])^:=FToken;
  pcardinal(@FData[9])^:=FDataIndex-12;
  if FSock.SendBuf(FData[1],FDataIndex)<>FDataIndex then Fail('Transmission error');
end;

function TRethinkDBConnection.ReadDoc(const dd: IJSONDocument): TResponseType;
var
  i,l:cardinal;
  d:UTF8String;
begin
  if FSock.ReceiveBuf(FData[1],12)<>12 then Fail('Transmission error');

  if pint64(@FData[1])^<>FToken then Fail('Response out of order');

  l:=pcardinal(@FData[9])^;
  i:=1;
  SetLength(d,l);
  while i<l do inc(i,FSock.ReceiveBuf(d[i],l-i+1));

  dd.Clear;
  dd.Parse(d);

  Result:=dd['t'];
  case Result of
    ResponseType_CLIENT_ERROR: raise ERethinkDBClientError.CreateFromDoc(dd);
    ResponseType_COMPILE_ERROR:raise ERethinkDBCompileError.CreateFromDoc(dd);
    ResponseType_RUNTIME_ERROR:raise ERethinkDBRuntimeError.CreateFromDoc(dd);
  end;
end;

function TRethinkDBConnection.ServerInfo: IJSONDocument;
var
  d:IJSONDocument;
begin
  FDataIndex:=12;
  Build('[5,[],{}]');
  inc(FToken);
  pint64(@FData[1])^:=FToken;
  pcardinal(@FData[9])^:=FDataIndex-12;
  if FSock.SendBuf(FData[1],FDataIndex)<>FDataIndex then Fail('Transmission error');

  d:=JSON;
  if ReadDoc(d)=ResponseType_SERVER_INFO then
    Result:=JSON(d['r'][0])
  else
    raise ERethinkDBError.Create('Unexpected response type');
end;

{ ERethinkDBErrorCode }

constructor ERethinkDBErrorCode.Create(const Msg: string; Code: integer);
begin
  inherited Create(Msg);
  FCode:=Code;
end;

constructor ERethinkDBErrorCode.CreateFromDoc(const d: IJSONDocument);
var
  a:OleVariant;
  i,j:integer;
  s:string;
begin
  a:=d['r'];
  if VarIsArray(a) then
   begin
    i:=VarArrayLowBound(a,1);
    j:=VarArrayHighBound(a,1);
    if i=j then
      s:=VarToStr(a[i])
    else
     begin
      s:=VarToStr(a[i]);
      inc(i);
      inc(j);
      while i<>j do
       begin
        s:=s+#13#10+VarToStr(a[i]);
        inc(i);
       end;
     end;
   end
  else
    s:=VarToStr(a);
  inherited Create(s);
  try
    FCode:=d['e'];
  except
    //on EVariantCovertError?
    FCode:=-1;
  end;
end;

{ TRethinkDBResultSet }

function TRethinkDBResultSet.Get(const d: IJSONDocument): boolean;
begin
  /////
  //d.Clear;
  Result:=false;
end;

{ TRetinkDB_DB }

function TRethinkDB_DB.table(const TableName: WideString;
  const Options: IJSONDocument): IRethinkDBTable;
begin
  Result:=TRethinkDBTable.Create(TermType_TABLE,
    Self.Chain(r.x(TableName)),Options);
end;

function TRethinkDB_DB.tableCreate(const TableName: WideString;
  const Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_TABLE_CREATE,
    Self.Chain(r.x(TableName)),Options);
end;

function TRethinkDB_DB.tableDrop(
  const TableName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_TABLE_DROP,
    Self.Chain(r.x(TableName)));
end;

function TRethinkDB_DB.tableList: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_TABLE_LIST,Self.Chain(nil));
end;

{ TRethinkDBTable }

function TRethinkDBTable.indexDrop(
  const IndexName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_DROP,
    Self.Chain(r.x(IndexName)));
end;

function TRethinkDBTable.indexList: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_LIST,Self.Chain(nil));
end;

function TRethinkDBTable.indexRename(const OldName,
  NewName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_RENAME,
    Self.Chain(r.x(OldName).Chain(r.x(NewName))));
end;

function TRethinkDBTable.get(const Key: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_GET,Self.Chain(r.x(Key)));
end;

function TRethinkDBTable.getAll: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_GET_ALL,Self.Chain(nil));
end;

end.
