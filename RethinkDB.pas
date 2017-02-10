unit RethinkDB;

{$D-}
{$L-}

interface

uses SysUtils, simpleSock, ql2, jsonDoc;

type
  TRethinkDB=class;//forward
  IRethinkDB=interface;//forward
  IRethinkDBTable=interface;//forward
  IRethinkDBTerm=interface;//forward
  IRethinkDBDatum=interface;//forward
  IRethinkDBStream=interface;//forward

  //////////////////////////////////////////////////////
  //// By default no "r" is declared, but if you want,
  //// include this in your project:
  //
  //r=TRethinkDB;

  TRetinkDBTerms=array of IRethinkDBTerm;

  TRethinkDB=class(TObject)
  protected
    class function x(const s:WideString):IRethinkDBTerm; overload;
    class function x(b:boolean):IRethinkDBTerm; overload;
    class function x(v:integer):IRethinkDBTerm; overload;
    class function x(d:IJSONDocument):IRethinkDBTerm; overload;
    class function xa(const p:IRethinkDBTerm;const a:array of WideString):TRetinkDBTerms;
    class function xx(const v:OleVariant):IRethinkDBTerm;
  public
    class function db(const DBName:WideString;
      const Options:IJSONDocument=nil):IRethinkDB;
    class function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    class function dbCreate(const DBName:WideString):IRethinkDBTerm;
    class function dbDrop(const DBName:WideString):IRethinkDBTerm;
    class function dbList:IRethinkDBTerm;

    class function row:IRethinkDBDatum; overload;
    class function row(const RowName:WideString):IRethinkDBDatum; overload;

    class function map(const sequence:IRethinkDBTerm;const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    class function map(const sequences:array of IRethinkDBTerm;const fn:IRethinkDBTerm):IRethinkDBStream; overload;

    class function uuid(const Input:WideString=''):IRethinkDBDatum;
    class function http(const URL:WideString;const Options:IJSONDocument=nil):IRethinkDBDatum;
  end;

  {$IFDEF DEBUG}
  TRethinkDBConnectionListener=procedure(Sender:TObject;InOut:boolean;Token:int64;const Data:UTF8String) of object;
  {$ENDIF}

  TRethinkDBConnection=class(TObject)
  private
    FSock:TTcpSocket;
    FData:UTF8String;
    FDataSize,FDataIndex:cardinal;
    FToken:int64;//TODO: async: not here!
    {$IFDEF DEBUG}
    FListener:TRethinkDBConnectionListener;
    {$ENDIF}
    function IsConnected:boolean;
    procedure Fail(const x:string);
    function AuthEx(const d:IJSONDocument):IJSONDocument;
    function SendTerm(const t:IRethinkDBTerm):int64;
    procedure Build(const s:UTF8String);
    function ReadDoc(token:int64;const dd: IJSONDocument): TResponseType;
    procedure SendSimple(token:int64;qt:TQueryType);
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
    {$IFDEF DEBUG}
    property Listener:TRethinkDBConnectionListener read FListener write FListener;
    {$ENDIF}
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
    procedure Chain(Next:IRethinkDBTerm);
    function Next:IRethinkDBTerm;
    procedure Build(b:TRethinkDBBuilder);
  end;

  IRethinkDBBool=interface;//forward

  IRethinkDBDatum=interface(IRethinkDBTerm)
    function eq(const v:OleVariant):IRethinkDBBool;
    function ne(const v:OleVariant):IRethinkDBBool;
    function lt(const v:OleVariant):IRethinkDBBool;
    function le(const v:OleVariant):IRethinkDBBool;
    function gt(const v:OleVariant):IRethinkDBBool;
    function ge(const v:OleVariant):IRethinkDBBool;

    //number
    function and_(const v:OleVariant):IRethinkDBDatum;
    function sub_(const v:OleVariant):IRethinkDBDatum;
    function mul_(const v:OleVariant):IRethinkDBDatum;
    function div_(const v:OleVariant):IRethinkDBDatum;
    function mod_(const v:OleVariant):IRethinkDBDatum;

    function floor:IRethinkDBDatum;
    function ceil:IRethinkDBDatum;
    function round:IRethinkDBDatum;

    //string
    function concat(const v:OleVariant):IRethinkDBDatum;

    //array
    function append(const v:OleVariant):IRethinkDBDatum;
    function prepend(const v:OleVariant):IRethinkDBDatum;
    function difference(const v:OleVariant):IRethinkDBDatum;
    function innerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBDatum;
    function outerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBDatum;
    function zip:IRethinkDBDatum;
    function map(const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function map(const sequences:array of IRethinkDBTerm;const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function withFields(const selectors:array of WideString):IRethinkDBDatum;

  end;

  IRethinkDBBool=interface(IRethinkDBDatum)
    function not_:IRethinkDBBool;
  end;

  IRethinkDB=interface(IRethinkDBTerm)
    function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    function tableCreate(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTerm;
    function tableDrop(const TableName:WideString):IRethinkDBTerm;
    function tableList:IRethinkDBTerm;
  end;

  IRethinkDBSingleRowSelection=interface;//forward
  IRethinkDBSelection=interface;//forward
  IRethinkDBTableSlice=interface;//forward

  IRethinkDBSequence=interface(IRethinkDBTerm)
    function innerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBSequence;
    function outerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBSequence;
    function eqJoin(const leftFieldOrFunction,rightTable:IRethinkDBTerm;
      const Options:IJSONDocument=nil):IRethinkDBSequence;
    function withFields(const selectors:array of WideString):IRethinkDBSequence;
  end;

  IRethinkDBStream=interface(IRethinkDBSequence)
    function zip:IRethinkDBStream;
  end;

  IRethinkDBTable=interface(IRethinkDBSequence)
    //TODO: indexCreate
    function indexDrop(const IndexName:WideString):IRethinkDBTerm;
    function indexList:IRethinkDBTerm;
    function indexRename(const OldName,NewName:WideString):IRethinkDBTerm;
    function indexStatus:IRethinkDBTerm; overload;
    function indexStatus(const IndexNames:array of WideString):IRethinkDBTerm; overload;
    function indexWait:IRethinkDBTerm; overload;
    function indexWait(const IndexNames:array of WideString):IRethinkDBTerm; overload;

    function insert(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function insert(const docs:array of IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    //TODO: update(function)
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    //TODO: replace(function)
    function delete(const Options:IJSONDocument=nil):IRethinkDBTerm;
    function sync:IRethinkDBTerm;

    function get(const Key:WideString):IRethinkDBSingleRowSelection;
    function getAll:IRethinkDBSelection; overload;
    function getAll(const Keys:array of WideString;const Options:IJSONDocument=nil):IRethinkDBSelection; overload;

    function between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;

  end;

  IRethinkDBSelection=interface(IRethinkDBTerm)
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    //TODO: update(function)
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    //TODO: replace(function)
    function delete(const Options:IJSONDocument=nil):IRethinkDBTerm;
    function sync:IRethinkDBTerm;

    function filter(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBSElection; overload;
    function filter(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBSElection; overload;
  end;

  IRethinkDBSingleRowSelection=interface(IRethinkDBSelection)
    //
  end;

  IRethinkDBTableSlice=interface(IRethinkDBTerm)
    function between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;
  end;

  TRethinkDBTerm=class(TTHREADUNSAFEInterfacedObject,IRethinkDBTerm)//abstract
  private
    FNext:IRethinkDBTerm;
  protected
    function Execute(Connection:TRethinkDBConnection;
      const Options:IJSONDocument=nil):IJSONDocument;
    function Run(Connection:TRethinkDBConnection;
      const Options:IJSONDocument=nil):IRethinkDBResultSet;

    procedure Chain(Next:IRethinkDBTerm);
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
    constructor Create; overload;//hide
  protected
    procedure Build(b:TRethinkDBBuilder); override;
  public
    constructor Create(tt:TTermType;const arg:IRethinkDBTerm;
      const opt:IJSONDocument=nil); overload;
    constructor Create(tt:TTermType;const args:array of IRethinkDBTerm;
      const opt:IJSONDocument=nil); overload;
  end;

  TRethinkDBConstant=class(TRethinkDBTerm,IRethinkDBTerm)
  private
    FData:UTF8String;
  protected
    procedure Build(b:TRethinkDBBuilder); override;
  public
    constructor Create(const Literal:UTF8String);
  end;

  TRethinkDBDatum=class(TRethinkDBValue,IRethinkDBDatum)
    function eq(const v:OleVariant):IRethinkDBBool;
    function ne(const v:OleVariant):IRethinkDBBool;
    function lt(const v:OleVariant):IRethinkDBBool;
    function le(const v:OleVariant):IRethinkDBBool;
    function gt(const v:OleVariant):IRethinkDBBool;
    function ge(const v:OleVariant):IRethinkDBBool;

    function and_(const v:OleVariant):IRethinkDBDatum;
    function sub_(const v:OleVariant):IRethinkDBDatum;
    function mul_(const v:OleVariant):IRethinkDBDatum;
    function div_(const v:OleVariant):IRethinkDBDatum;
    function mod_(const v:OleVariant):IRethinkDBDatum;

    function floor:IRethinkDBDatum;
    function ceil:IRethinkDBDatum;
    function round:IRethinkDBDatum;
    
    function concat(const v:OleVariant):IRethinkDBDatum;

    function append(const v:OleVariant):IRethinkDBDatum;
    function prepend(const v:OleVariant):IRethinkDBDatum;
    function difference(const v:OleVariant):IRethinkDBDatum;

    function innerJoin(const otherArray,predicate:IRethinkDBTerm):IRethinkDBDatum;
    function outerJoin(const otherArray,predicate:IRethinkDBTerm):IRethinkDBDatum;
    function zip:IRethinkDBDatum;
    function map(const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function map(const sequences:array of IRethinkDBTerm;const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function withFields(const selectors:array of WideString):IRethinkDBDatum;
  end;

  TRethinkDBBool=class(TRethinkDBDatum,IRethinkDBBool)
    function not_:IRethinkDBBool;
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

  TRethinkDBSequence=class(TRethinkDBValue,IRethinkDBSequence)
    function innerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBSequence;
    function outerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBSequence;
    function eqJoin(const leftFieldOrFunction,rightTable:IRethinkDBTerm;
      const Options:IJSONDocument=nil):IRethinkDBSequence;
    function zip:IRethinkDBSequence;
    function withFields(const selectors:array of WideString):IRethinkDBSequence;
  end;

  TRethinkDBTable=class(TRethinkDBSequence,IRethinkDBTable)
    function indexDrop(const IndexName:WideString):IRethinkDBTerm;
    function indexList:IRethinkDBTerm;
    function indexRename(const OldName,NewName:WideString):IRethinkDBTerm;
    function indexStatus:IRethinkDBTerm; overload;
    function indexStatus(const IndexNames:array of WideString):IRethinkDBTerm; overload;
    function indexWait:IRethinkDBTerm; overload;
    function indexWait(const IndexNames:array of WideString):IRethinkDBTerm; overload;

    function insert(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function insert(const docs:array of IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function delete(const Options:IJSONDocument=nil):IRethinkDBTerm;
    function sync:IRethinkDBTerm;

    function get(const Key:WideString):IRethinkDBSingleRowSelection;
    function getAll:IRethinkDBSelection; overload;
    function getAll(const Keys:array of WideString;const Options:IJSONDocument=nil):IRethinkDBSelection; overload;

    function between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;
  end;

  TRethinkDBSelection=class(TRethinkDBValue,IRethinkDBSelection)
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBTerm; overload;
    function delete(const Options:IJSONDocument=nil):IRethinkDBTerm;
    function sync:IRethinkDBTerm;

    function filter(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBSElection; overload;
    function filter(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBSElection; overload;
  end;

  TRethinkDBSingleRowSelection=class(TRethinkDBSelection,IRethinkDBSingleRowSelection)
  end;

  TRethinkDBTableSlice=class(TRethinkDBValue,IRethinkDBTableSlice)
    function between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;
  end;

  TRethinkDBStream=class(TRethinkDBSequence,IRethinkDBStream)
    function zip:IRethinkDBStream;
  end;

  TRethinkDBResultSet=class(TTHREADUNSAFEInterfacedObject,IRethinkDBResultSet)
  private
    FConnection:TRethinkDBConnection;
    FToken:int64;
  protected
    constructor Create(rdb:TRethinkDBConnection;token:int64);
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

class function TRethinkDB.xx(const v: OleVariant): IRethinkDBTerm;
var
  vt:TVarType;
  i,j,k:integer;
  a:array of IRethinkDBTerm;
begin
  vt:=VarType(v);
  if (vt and varArray)=0 then
    case VarType(v) of
      varEmpty,varNull:
        Result:=TRethinkDBConstant.Create('null');
      varSmallint,varInteger,varSingle,varDouble,varCurrency,
      $000E,//varDecimal
      varShortInt,varByte,varWord,varLongWord,varInt64,
      $0015://varWord64
        Result:=TRethinkDBConstant.Create(UTF8Encode(VarToWideStr(v)));
      //varDate://TODO
      varOleStr:
        Result:=x(VarToWideStr(v));
      //varDispatch,varUnknown://TODO
        //IRethinkDBTerm...
        //IJSONDocument...
      //varError:?
      varBoolean:
        Result:=x(boolean(v));
      //varVariant:?

      //varStrArg //?

      //varTypeMask = $0FFF;
      //varArray    = $2000;
      //varByRef    = $4000;

      else raise ERethinkDBError.Create('Unsupported VarType '+IntToHex(VarType(v),4));
    end
  else
   begin
    if VarArrayDimCount(v)<>1 then raise ERethinkDBError.Create('Only 1-dimension arrays supported');
    i:=VarArrayLowBound(v,1);
    j:=VarArrayHighBound(v,1)+1;
    k:=0;
    SetLength(a,j-i);
    while i<>j do
     begin
      a[k]:=xx(v[i]);
      inc(i);
      inc(k);
     end;
    Result:=TRethinkDBValue.Create(TermType_MAKE_ARRAY,a); 
   end;
end;

class function TRethinkDB.xa(const p: IRethinkDBTerm;
  const a: array of WideString): TRetinkDBTerms;
var
  i,l:integer;
begin
  l:=Length(a);
  SetLength(Result,l+1);
  Result[0]:=p;
  i:=0;
  while i<>l do
   begin
    Result[i+1]:=x(a[i]);
    inc(i);
   end;
end;

class function TRethinkDB.x(d: IJSONDocument): IRethinkDBTerm;
begin
  //Result:=TRethinkDBValue.Create(TermType_JSON,r.x(d.ToString));//?
  Result:=TRethinkDBConstant.Create(UTF8Encode(d.ToString));
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

class function TRethinkDB.dbCreate(const DBName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DB_CREATE,x(DBName));
end;

class function TRethinkDB.dbDrop(const DBName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DB_DROP,x(DBName));
end;

class function TRethinkDB.dbList: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DB_LIST,nil);
end;

class function TRethinkDB.uuid(const Input: WideString): IRethinkDBDatum;
begin
  if Input='' then
    Result:=TRethinkDBDatum.Create(TermType_UUID,nil)
  else
    Result:=TRethinkDBDatum.Create(TermType_UUID,x(Input));
end;

class function TRethinkDB.row: IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_IMPLICIT_VAR,nil);
end;

class function TRethinkDB.row(const RowName: WideString): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_GET_FIELD,[row,x(RowName)]);
end;

class function TRethinkDB.http(const URL: WideString; const Options: IJSONDocument): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_HTTP,[x(URL)],Options);
end;

class function TRethinkDB.map(const sequence, fn: IRethinkDBTerm): IRethinkDBStream;
begin
  Result:=TRethinkDBStream.Create(TermType_MAP,[sequence,fn]);
end;

class function TRethinkDB.map(const sequences: array of IRethinkDBTerm;
  const fn: IRethinkDBTerm): IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  for i:=0 to l-1 do a[i]:=sequences[i];
  a[l]:=fn;
  Result:=TRethinkDBStream.Create(TermType_MAP,a);
end;

{ TRethinkDBTerm }

constructor TRethinkDBTerm.Create;
begin
  inherited Create;
  FNext:=nil;
end;

procedure TRethinkDBTerm.Chain(Next: IRethinkDBTerm);
begin
  //if FNext<>nil then raise?
  FNext:=Next;
end;

function TRethinkDBTerm.Next: IRethinkDBTerm;
begin
  Result:=FNext;
end;

function TRethinkDBTerm.Execute(Connection: TRethinkDBConnection; const Options: IJSONDocument): IJSONDocument;
var
  token:int64;
begin
  token:=Connection.SendTerm(Self);
  Result:=JSON;
  Connection.ReadDoc(token,Result);
  {//TODO
  case  of
    ResponseType_SUCCESS_ATOM:;
    else raise?
  end;
  }
end;

function TRethinkDBTerm.Run(Connection: TRethinkDBConnection;
  const Options: IJSONDocument): IRethinkDBResultSet;
begin
  Result:=TRethinkDBResultSet.Create(Connection,Connection.SendTerm(Self));
end;

{ TRethinkDBValue }

constructor TRethinkDBValue.Create;
begin
  raise ERethinkDBError.Create('Call one of the other constructors');
end;

constructor TRethinkDBValue.Create(tt:TTermType;const arg:IRethinkDBTerm;
  const opt:IJSONDocument=nil);
begin
  inherited Create;
  FTermType:=tt;
  FFirstArg:=arg;
  if arg<>nil then arg.Chain(nil);
  FOptions:=opt;
end;

constructor TRethinkDBValue.Create(tt:TTermType;const args:array of IRethinkDBTerm; const opt:IJSONDocument=nil);
var
  i,l:integer;
begin
  inherited Create;
  FTermType:=tt;
  l:=Length(args);
  if l=0 then
    FFirstArg:=nil
  else
   begin
    FFirstArg:=args[0];
    for i:=0 to l-2 do args[i].Chain(args[i+1]);
    args[l-1].Chain(nil);
   end;
  FOptions:=opt;
end;

procedure TRethinkDBValue.Build(b: TRethinkDBBuilder);
var
  v:IRethinkDBTerm;
begin
  b('[');
  b(IntToStr(integer(FTermType)));
  if FFirstArg=nil then
    //b(',[]')//?
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
    b(',');
    b(FOptions.ToString);//BAD!!!
   end;
  b(']');
end;

{ r }

type
  r=TRethinkDB;

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

{ TRethinkDBDatum }

function TRethinkDBDatum.eq(const v: OleVariant): IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_EQ,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.ne(const v: OleVariant): IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_NE,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.lt(const v: OleVariant): IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_LT,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.le(const v: OleVariant): IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_LE,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.gt(const v: OleVariant): IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_GT,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.ge(const v: OleVariant): IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_GE,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.and_(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_AND,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.sub_(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_SUB,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.mul_(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MUL,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.div_(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_DIV,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.mod_(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MOD,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.floor: IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_FLOOR,Self);
end;

function TRethinkDBDatum.ceil: IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_CEIL,Self);
end;

function TRethinkDBDatum.round: IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_ROUND,Self);
end;

function TRethinkDBDatum.concat(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBBool.Create(TermType_AND,[Self,r.xx(v)]);//yes it's "AND", see ql2.proto
end;

function TRethinkDBDatum.append(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_APPEND,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.prepend(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_PREPEND,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.difference(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_DIFFERENCE,[Self,r.xx(v)]);
end;

function TRethinkDBDatum.innerJoin(const otherArray, predicate: IRethinkDBTerm): IRethinkDBDatum;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBDatum.Create(TermType_INNER_JOIN,[Self,otherArray,predicate]);
end;

function TRethinkDBDatum.outerJoin(const otherArray, predicate: IRethinkDBTerm): IRethinkDBDatum;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBDatum.Create(TermType_INNER_JOIN,[Self,otherArray,predicate]);
end;

function TRethinkDBDatum.zip: IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_Zip,[Self]);
end;

function TRethinkDBDatum.map(const fn: IRethinkDBTerm): IRethinkDBStream;
begin
  Result:=TRethinkDBStream.Create(TermType_MAP,[Self,fn]);
end;

function TRethinkDBDatum.map(const sequences: array of IRethinkDBTerm;
  const fn: IRethinkDBTerm): IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+2);
  a[0]:=Self;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  a[l+1]:=fn;
  Result:=TRethinkDBStream.Create(TermType_MAP,a);
end;

function TRethinkDBDatum.withFields(const selectors: array of WideString): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_WITH_FIELDS,r.xa(Self,selectors));
end;

{ TRethinkDBBool }

function TRethinkDBBool.not_: IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_NOT,Self);
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
  {$IFDEF DEBUG}
  FListener:=nil;
  {$ENDIF}
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

function TRethinkDBConnection.SendTerm(const t: IRethinkDBTerm): int64;
begin
  //TODO: lock?

  FDataIndex:=12;
  Build('[1,');//QueryType_START
  t.Build(Build);
  Build(']');//Build(',{}]');

  inc(FToken);
  pint64(@FData[1])^:=FToken;
  pcardinal(@FData[9])^:=FDataIndex-12;
  if FSock.SendBuf(FData[1],FDataIndex)<>FDataIndex then Fail('Transmission error');
  Result:=FToken;

  {$IFDEF DEBUG}
  if @FListener<>nil then FListener(Self,false,FToken,Copy(FData,13,FDataIndex-12));
  {$ENDIF}
end;

function TRethinkDBConnection.ReadDoc(token:int64;const dd: IJSONDocument): TResponseType;
var
  i,l:cardinal;
  d:UTF8String;
begin
  if FSock.ReceiveBuf(FData[1],12)<>12 then Fail('Transmission error');

  //todo: async!
  if pint64(@FData[1])^<>token then Fail('Response out of order');

  l:=pcardinal(@FData[9])^;
  i:=1;
  SetLength(d,l);
  while i<l do inc(i,FSock.ReceiveBuf(d[i],l-i+1));

  {$IFDEF DEBUG}
  if @FListener<>nil then FListener(Self,true,FToken,d);
  {$ENDIF}

  dd.Clear;
  dd.Parse(d);

  Result:=dd['t'];
  case Result of
    ResponseType_CLIENT_ERROR: raise ERethinkDBClientError.CreateFromDoc(dd);
    ResponseType_COMPILE_ERROR:raise ERethinkDBCompileError.CreateFromDoc(dd);
    ResponseType_RUNTIME_ERROR:raise ERethinkDBRuntimeError.CreateFromDoc(dd);
  end;
end;

procedure TRethinkDBConnection.SendSimple(token:int64;qt:TQueryType);
begin
  FDataIndex:=12;
  Build('['+IntToStr(integer(qt))+']');
  pint64(@FData[1])^:=token;
  pcardinal(@FData[9])^:=FDataIndex-12;
  if FSock.SendBuf(FData[1],FDataIndex)<>FDataIndex then Fail('Transmission error');

  {$IFDEF DEBUG}
  if @FListener<>nil then FListener(Self,false,FToken,Copy(FData,13,FDataIndex-12));
  {$ENDIF}
end;

function TRethinkDBConnection.ServerInfo: IJSONDocument;
var
  d:IJSONDocument;
begin
  inc(FToken);
  SendSimple(FToken,QueryType_SERVER_INFO);
  d:=JSON;
  if ReadDoc(FToken,d)=ResponseType_SERVER_INFO then
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
  //assert d['t'] in [ResponseType_CLIENT_ERROR,ResponseType_COMPILE_ERROR,ResponseType_RUNTIME_ERROR];
  a:=d['r'];
  if VarIsArray(a) then
   begin
    //if VarArrayDimCount(a)<>1 then raise ERethinkDBError.Create('Only 1-dimension arrays supported');
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
    a:=d['e'];
    if VarIsNull(a) then FCode:=-1 else FCode:=a;
  except
    //on EVariantCovertError?
    FCode:=-1;
  end;
end;

{ TRethinkDBResultSet }

constructor TRethinkDBResultSet.Create(rdb: TRethinkDBConnection;
  token: int64);
begin
  inherited Create;
  FConnection:=rdb;
  FToken:=token;
end;

function TRethinkDBResultSet.Get(const d: IJSONDocument): boolean;
var
  r:TResponseType;
begin
  d.Clear;

  FConnection.SendSimple(FToken,QueryType_CONTINUE);
  r:=FConnection.ReadDoc(FToken,d);

  //Result:=r=ResponseType_SUCCESS_SEQUENCE;
  Result:=r<>ResponseType_RUNTIME_ERROR;
end;

{ TRetinkDB_DB }

function TRethinkDB_DB.table(const TableName: WideString; const Options: IJSONDocument): IRethinkDBTable;
begin
  Result:=TRethinkDBTable.Create(TermType_TABLE,[Self,r.x(TableName)],Options);
end;

function TRethinkDB_DB.tableCreate(const TableName: WideString;
  const Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_TABLE_CREATE,[Self,r.x(TableName)],Options);
end;

function TRethinkDB_DB.tableDrop(
  const TableName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_TABLE_DROP,[Self,r.x(TableName)]);
end;

function TRethinkDB_DB.tableList: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_TABLE_LIST,Self);
end;

{ TRethinkDBTable }

function TRethinkDBTable.indexDrop(
  const IndexName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_DROP,[Self,r.x(IndexName)]);
end;

function TRethinkDBTable.indexList: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_LIST,Self);
end;

function TRethinkDBTable.indexRename(const OldName,
  NewName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_RENAME,[Self,r.x(OldName),r.x(NewName)]);
end;

function TRethinkDBTable.indexStatus: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_STATUS,Self);
end;

function TRethinkDBTable.indexStatus(const IndexNames: array of WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_STATUS,r.xa(Self,IndexNames));
end;

function TRethinkDBTable.indexWait: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_WAIT,Self);
end;

function TRethinkDBTable.indexWait(
  const IndexNames: array of WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INDEX_WAIT,r.xa(Self,IndexNames));
end;

function TRethinkDBTable.get(const Key: WideString): IRethinkDBSingleRowSelection;
begin
  Result:=TRethinkDBSingleRowSelection.Create(TermType_GET,[Self,r.x(Key)]);
end;

function TRethinkDBTable.getAll:IRethinkDBSelection;
begin
  Result:=TRethinkDBSelection.Create(TermType_GET_ALL,[Self]);
end;

function TRethinkDBTable.getAll(const Keys:array of WideString;const Options:IJSONDocument=nil):IRethinkDBSelection;
begin
  Result:=TRethinkDBSelection.Create(TermType_GET_ALL,r.xa(Self,Keys),Options);
end;

function TRethinkDBTable.insert(const doc, Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_INSERT,[Self,r.x(doc)],Options);
end;

function TRethinkDBTable.insert(const docs: array of IJSONDocument; const Options: IJSONDocument): IRethinkDBTerm;
var
  a:array of IRethinkDBTerm;
  i,l:integer;
begin
  l:=Length(docs);
  SetLength(a,l);
  for i:=0 to l-1 do a[i]:=r.x(docs[i]);
  Result:=TRethinkDBValue.Create(TermType_INSERT,[Self,
    TRethinkDBValue.Create(TermType_MAKE_ARRAY,a)],Options);
end;

function TRethinkDBTable.update(const doc, Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_UPDATE,[Self,r.x(doc)],Options);
end;

function TRethinkDBTable.replace(const doc, Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_REPLACE,[Self,r.x(doc)],Options);
end;

function TRethinkDBTable.delete(const Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DELETE,Self,Options);
end;

function TRethinkDBTable.sync: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_SYNC,Self);
end;

function TRethinkDBTable.between(const LowerKey, UpperKey: WideString;
  const Options: IJSONDocument): IRethinkDBTableSlice;
begin
  Result:=TRethinkDBTableSlice.Create(TermType_BETWEEN,[Self,r.x(LowerKey),r.x(UpperKey)],Options);
end;

{ TRethinkDBSelection }

function TRethinkDBSelection.update(const doc, Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_UPDATE,[Self,r.x(doc)],Options);
end;

function TRethinkDBSelection.replace(const doc, Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_REPLACE,[Self,r.x(doc)],Options);
end;

function TRethinkDBSelection.delete(const Options: IJSONDocument): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DELETE,Self,Options);
end;

function TRethinkDBSelection.sync: IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_SYNC,Self);
end;

function TRethinkDBSelection.filter(const KeyValue, Options: IJSONDocument): IRethinkDBSElection;
var
  e:IJSONEnumerator;
  a,b:IRethinkDBTerm;
begin
  e:=JSONEnum(KeyValue);
  a:=nil;
  while e.Next do
   begin
    b:=TRethinkDBValue.Create(TermType_EQ,
      [TRethinkDBValue.Create(TermType_GET_FIELD,
        [TRethinkDBValue.Create(TermType_VAR,r.x(1))
        ,r.x(e.Key)])
      ,r.xx(e.Value)]);
    //TODO: if e.Value is IJSONDocument...
    if a=nil then a:=b else a:=TRethinkDBValue.Create(TermType_AND,[a,b]);
   end;
  if a=nil then a:=r.x(true);
  Result:=TRethinkDBSelection.Create(TermType_FILTER,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),a])],Options);
end;

function TRethinkDBSelection.filter(const Predicate: IRethinkDBTerm;
  const Options: IJSONDocument): IRethinkDBSElection;
begin
  Result:=TRethinkDBSelection.Create(TermType_Filter,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),Predicate])],Options);
end;

{ TRethinkDBTableSlice }

function TRethinkDBTableSlice.between(const LowerKey, UpperKey: WideString;
  const Options: IJSONDocument): IRethinkDBTableSlice;
begin
  Result:=TRethinkDBTableSlice.Create(TermType_BETWEEN,[Self,r.x(LowerKey),r.x(UpperKey)],Options);
end;

{ TRethinkDBSequence }

function TRethinkDBSequence.innerJoin(const otherSequence, predicate: IRethinkDBTerm): IRethinkDBSequence;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBSequence.Create(TermType_INNER_JOIN,[Self,otherSequence,predicate]);
end;

function TRethinkDBSequence.outerJoin(const otherSequence, predicate: IRethinkDBTerm): IRethinkDBSequence;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBSequence.Create(TermType_OUTER_JOIN,[Self,otherSequence,predicate]);
end;

function TRethinkDBSequence.eqJoin(const leftFieldOrFunction, rightTable: IRethinkDBTerm;
  const Options: IJSONDocument): IRethinkDBSequence;
begin
  Result:=TRethinkDBSequence.Create(TermType_EQ_JOIN,[Self,leftFieldOrFunction,rightTable],Options);
end;

function TRethinkDBSequence.zip: IRethinkDBSequence;
begin
  Result:=TRethinkDBSequence.Create(TermType_ZIP,[Self]);
end;

{ TRethinkDBStream }

function TRethinkDBSequence.withFields(const selectors: array of WideString): IRethinkDBSequence;
begin
  Result:=TRethinkDBSequence.Create(TermType_WITH_FIELDS,r.xa(Self,selectors));
end;

{ TRethinkDBStream }

function TRethinkDBStream.zip: IRethinkDBStream;
begin
  Result:=TRethinkDBStream.Create(TermType_Zip,[Self]);
end;

end.
