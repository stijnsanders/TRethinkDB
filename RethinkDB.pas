unit RethinkDB;

{$D-}
{$L-}

interface

uses SysUtils, simpleSock, ql2, jsonDoc;

type
  TRethinkDB=class;//forward
  IRethinkDBTerm=interface;//forward
  IRethinkDBDatabase=interface;//forward
  IRethinkDBDatum=interface;//forward
  IRethinkDBBool=interface;//forward
  IRethinkDBArray=interface;//forward
  IRethinkDBObject=interface;//forward
  IRethinkDBSequence=interface;//forward
  IRethinkDBStream=interface;//forward
  IRethinkDBTable=interface;//forward

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
      const Options:IJSONDocument=nil):IRethinkDBDatabase;
    class function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    class function dbCreate(const DBName:WideString):IRethinkDBObject;
    class function dbDrop(const DBName:WideString):IRethinkDBObject;
    class function dbList:IRethinkDBArray;

    class function row:IRethinkDBDatum; overload;
    class function row(const FieldName:WideString):IRethinkDBDatum; overload;
    //TODO: property field[const FieldName:WideString]:IRethinkDBDatum read row; default;

    class function fn(const FnBody:IRethinkDBTerm;ArgCount:integer=1):IRethinkDBTerm; overload;
    class function fn(const FnBody:IJSONDocument;ArgCount:integer=1):IRethinkDBTerm; overload;
    class function arg(const ArgIndex:integer):IRethinkDBDatum; overload;
    class function arg(const ArgIndex:integer;const FieldName:WideString):IRethinkDBDatum; overload;

    class function map(const sequences:array of IRethinkDBSequence;const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    class function map(const arrays:array of IRethinkDBArray;const fn:IRethinkDBTerm):IRethinkDBArray; overload;
    class function union(const stream:IRethinkDBStream;
      const sequences:array of IRethinkDBSequence):IRethinkDBStream; overload;
    class function union(const stream:IRethinkDBStream; const sequences:array of IRethinkDBSequence;
      const interleave:OleVariant):IRethinkDBStream; overload;
    class function union(const array_:IRethinkDBDatum;
      const sequences:array of IRethinkDBSequence):IRethinkDBDatum; overload;
    class function union(const array_:IRethinkDBDatum; const sequences:array of IRethinkDBSequence;
      const interleave:OleVariant):IRethinkDBDatum; overload;

    class function uuid(const Input:WideString=''):IRethinkDBDatum;
    class function http(const URL:WideString;const Options:IJSONDocument=nil):IRethinkDBDatum;
    class function asc(const FieldName:WideString):IRethinkDBTerm;
    class function desc(const FieldName:WideString):IRethinkDBTerm;

    class function group(const sequence:IRethinkDBSequence;
      const field:WideString;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    class function group(const sequence:IRethinkDBSequence;
      const func:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    class function reduce(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum;
    class function count(x:IRethinkDBTerm):IRethinkDBDatum;
    class function sum(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum; overload;
    class function sum(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    class function avg(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum; overload;
    class function avg(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    class function min(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum; overload;
    class function min(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    class function min_index(const sequence:IRethinkDBSequence;const indexName:WideString):IRethinkDBDatum; overload;
    class function max(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum; overload;
    class function max(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    class function max_index(const sequence:IRethinkDBSequence;const indexName:WideString):IRethinkDBDatum; overload;
    class function distinct(const sequence:IRethinkDBSequence):IRethinkDBArray; overload;
    class function distinct(const table:IRethinkDBTable;const indexName:WideString=''):IRethinkDBStream; overload;
    class function contains(const sequence:IRethinkDBSequence;const v:OleVariant):IRethinkDBBool; overload;
    class function contains(const sequence:IRethinkDBSequence;const vv:array of OleVariant):IRethinkDBBool; overload;
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
    ['{914532C9-49C6-4A46-81E5-40DA7EF7D1BD}']
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

  IRethinkDBDatum=interface(IRethinkDBTerm)
    ['{F956274B-6368-48B4-805F-E93EEF83A856}']
    function eq(const v:OleVariant):IRethinkDBBool;
    function ne(const v:OleVariant):IRethinkDBBool;
    function lt(const v:OleVariant):IRethinkDBBool;
    function le(const v:OleVariant):IRethinkDBBool;
    function gt(const v:OleVariant):IRethinkDBBool;
    function ge(const v:OleVariant):IRethinkDBBool;

    //number
    function add_(const v:OleVariant):IRethinkDBDatum;
    function sub_(const v:OleVariant):IRethinkDBDatum;
    function mul_(const v:OleVariant):IRethinkDBDatum;
    function div_(const v:OleVariant):IRethinkDBDatum;
    function mod_(const v:OleVariant):IRethinkDBDatum;

    function floor:IRethinkDBDatum;
    function ceil:IRethinkDBDatum;
    function round:IRethinkDBDatum;

    //string
    function concat(const v:OleVariant):IRethinkDBDatum;
    function slice_n1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBDatum;
    function slice_n(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBDatum;
    function count: IRethinkDBDatum;

    //array see IRethinkDBArray

    //object see IRethinkDBObject

    //function ungroup:IRethinkDBArray;//?

  end;

  IRethinkDBBool=interface(IRethinkDBDatum)
    ['{863FDB51-4D60-4369-AC91-6AD20F363C0C}']
    function and_(const b:IRethinkDBBool):IRethinkDBBool;
    function or_(const b:IRethinkDBBool):IRethinkDBBool;
    function not_:IRethinkDBBool;
  end;

  IRethinkDBArray=interface(IRethinkDBTerm)//*:
    //actually both IRethinkDBDatum and IRethinkDBSequence,
    //but interfaces don't do multiple inheritance
    //so reverted to first common parent IRethinkDBTerm
    //(single TRethinkDBTerm does implementation anyway)
    ['{18DF4808-F0DE-40BB-86A5-C967FE43CB9A}']

    function filter_a(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBArray; overload;
    function filter_a(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBArray; overload;
    function innerJoin(const otherArray,predicate:IRethinkDBTerm):IRethinkDBArray;
    function outerJoin(const otherArray,predicate:IRethinkDBTerm):IRethinkDBArray;

    function append(const v:OleVariant):IRethinkDBDatum;
    function prepend(const v:OleVariant):IRethinkDBDatum;
    function difference(const v:OleVariant):IRethinkDBDatum;
    function zip:IRethinkDBArray;
    function map(const fn:IRethinkDBTerm):IRethinkDBArray; overload;
    function map(const arrays:array of IRethinkDBArray;const fn:IRethinkDBTerm):IRethinkDBArray; overload;
    function withFields(const selectors:array of WideString):IRethinkDBArray;
    function concatMap(const fn:IRethinkDBTerm):IRethinkDBArray; overload;
    function skip(n:integer):IRethinkDBArray;
    function limit(n:integer):IRethinkDBArray;
    function slice_a1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBArray;
    function slice_a(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBArray;
    function union_a1(const sequence:IRethinkDBSequence):IRethinkDBArray;
    function union_a2(const sequence:IRethinkDBSequence;const interleave:OleVariant):IRethinkDBArray;
    function union_a(const sequences:array of IRethinkDBSequence):IRethinkDBArray;
    function union_a3(const sequences:array of IRethinkDBSequence;const interleave:OleVariant):IRethinkDBArray;
    function sample_a(n:integer):IRethinkDBArray;

    function count:IRethinkDBDatum;

  end;

  IRethinkDBObject=interface(IRethinkDBDatum)
    ['{025863D6-4BF0-4FB8-8E4D-F6A34FF022D9}']
    function count:IRethinkDBDatum;
  end;

  IRethinkDBDatabase=interface(IRethinkDBTerm)
    ['{D3D670F6-C882-4935-9E10-7714E4E903FB}']
    function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    function tableCreate(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBObject;
    function tableDrop(const TableName:WideString):IRethinkDBObject;
    function tableList:IRethinkDBArray;
  end;

  IRethinkDBSelection=interface;//forward

  IRethinkDBSequence=interface(IRethinkDBTerm)
    ['{A1CD9948-CE84-45DF-8EFA-3EA5DCF5D172}']
    function innerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBStream;
    function outerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBStream;
    function eqJoin(const leftFieldOrFunction,rightTable:IRethinkDBTerm;
      const Options:IJSONDocument=nil):IRethinkDBSequence;
    function map(const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function map(const sequences:array of IRethinkDBSequence;const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function withFields(const selectors:array of WideString):IRethinkDBStream;
    //NOTICE: I've tried overload, but interface aliases can't handle overloads
    function orderBy_s1(const v:OleVariant):IRethinkDBArray;
    function orderBy_s(const vv:array of OleVariant):IRethinkDBArray;
    function skip(n:integer):IRethinkDBStream;
    function limit(n:integer):IRethinkDBStream;
    function nth_s(n:integer):IRethinkDBObject;
    function offsetsOf(DatumOrPredicate:OleVariant):IRethinkDBArray;
    function isEmpty:IRethinkDBBool;
    function sample_s(n:integer):IRethinkDBSelection;
    function group(const field:WideString;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function group(const func:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function reduce(const func:IRethinkDBTerm):IRethinkDBDatum;
    function fold(const base,func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function fold(const base,func,emit,finalEmit:IRethinkDBTerm):IRethinkDBSequence; overload;
    function sum(const field:WideString):IRethinkDBDatum; overload;
    function sum(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function avg(const field:WideString):IRethinkDBDatum; overload;
    function avg(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function min(const field:WideString):IRethinkDBDatum; overload;
    function min(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function min_index(const indexName:WideString):IRethinkDBDatum; overload;
    function max(const field:WideString):IRethinkDBDatum; overload;
    function max(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function max_index(const indexName:WideString):IRethinkDBDatum; overload;
    function distinct_s:IRethinkDBArray;
  end;

  IRethinkDBStream=interface(IRethinkDBSequence)
    ['{542AEDF9-A9DE-42DA-81DA-B85E87903518}']
    function changes(const Options:IJSONDocument=nil):IRethinkDBStream;
    function filter_s(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function filter_s(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function zip:IRethinkDBStream;
    function concatMap(const fn:IRethinkDBTerm):IRethinkDBStream;
    function slice_z1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBStream;
    function slice_z(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBStream;
    function union_z1(const sequence:IRethinkDBSequence):IRethinkDBStream;
    function union_z2(const sequence:IRethinkDBSequence;const interleave:OleVariant):IRethinkDBStream;
    function union_z(const sequences:array of IRethinkDBSequence):IRethinkDBStream;
    function union_z3(const sequences:array of IRethinkDBSequence;const interleave:OleVariant):IRethinkDBStream;
    function sample_z(n:integer):IRethinkDBArray;
    function ungroup:IRethinkDBDatum;
  end;

  IRethinkDBSelection=interface(IRethinkDBStream)
    ['{DED262BA-4C88-46CD-86C4-83F1E9E8AEAA}']
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function update(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function delete(const Options:IJSONDocument=nil):IRethinkDBObject;
    function sync:IRethinkDBObject;

    function filter_x(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBSelection; overload;
    function filter_x(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBSelection; overload;

    function orderBy_x1(const v:OleVariant):IRethinkDBSelection{<IRethinkDBArray>};
    function orderBy_x(const vv:array of OleVariant):IRethinkDBSelection{<IRethinkDBArray>};

    function slice_x1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBSelection;
    function slice_x(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBSelection;
    function nth_x(n:integer):IRethinkDBSelection{<IRethinkDBObject>};
  end;

  IRethinkDBSingleSelection=interface(IRethinkDBObject)
    ['{7D47E895-4DCC-4B5C-A956-3BCD777E3719}']
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function update(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function delete(const Options:IJSONDocument=nil):IRethinkDBObject;
  end;

  IRethinkDBTableSlice=interface(IRethinkDBSelection)
    ['{E7A1448D-3C62-42CB-A4A8-8DB333A31BC5}']
    function between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;
  end;

  IRethinkDBTable=interface(IRethinkDBSelection)
    ['{6A872B89-99D4-4102-8CAE-1F5D1831F15D}']
    function indexCreate(const IndexName:WideString;const IndexFunction:IRethinkDBTerm=nil;
      const Options:IJSONDocument=nil):IRethinkDBObject;
    function indexDrop(const IndexName:WideString):IRethinkDBObject;
    function indexList:IRethinkDBArray;
    function indexRename(const OldName,NewName:WideString):IRethinkDBObject;
    function indexStatus:IRethinkDBArray; overload;
    function indexStatus(const IndexNames:array of WideString):IRethinkDBTerm; overload;
    function indexWait:IRethinkDBArray; overload;
    function indexWait(const IndexNames:array of WideString):IRethinkDBTerm; overload;

    function insert(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function insert(const docs:array of IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;

    function get(const Key:WideString):IRethinkDBSingleSelection;
    function getAll(const Keys:array of WideString;const Options:IJSONDocument=nil):IRethinkDBSelection;

    function between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;
    function orderBy_t1(const v:OleVariant):IRethinkDBTableSlice;
    function orderBy_t(const vv:array of OleVariant):IRethinkDBTableSlice;

    function distinct_t(const indexName:WideString=''):IRethinkDBStream;
  end;

  //TODO: IRethinkDBBinary
  //slice,count


  /////////////
  // implementations

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

  TRethinkDBValueClass=class of TRethinkDBValue;

  TRethinkDBValue=class(TRethinkDBTerm,IRethinkDBTerm)
  private
    FTermType:TTermType;
    FFirstArg:IRethinkDBTerm;
    FOptions:IJSONDocument;
    constructor Create; overload;//hide
    function PrepOrderBy(rt:TRethinkDBValueClass;const vv:array of OleVariant):IRethinkDBTerm;
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

//https://rethinkdb.com/api/javascript/row/

  TRethinkDBDatum=class(TRethinkDBValue,IRethinkDBDatum,IRethinkDBArray,
    IRethinkDBObject,IRethinkDBSingleSelection)

    //IRethinkDBDatum
    function eq(const v:OleVariant):IRethinkDBBool;
    function ne(const v:OleVariant):IRethinkDBBool;
    function lt(const v:OleVariant):IRethinkDBBool;
    function le(const v:OleVariant):IRethinkDBBool;
    function gt(const v:OleVariant):IRethinkDBBool;
    function ge(const v:OleVariant):IRethinkDBBool;

    function add_(const v:OleVariant):IRethinkDBDatum;
    function sub_(const v:OleVariant):IRethinkDBDatum;
    function mul_(const v:OleVariant):IRethinkDBDatum;
    function div_(const v:OleVariant):IRethinkDBDatum;
    function mod_(const v:OleVariant):IRethinkDBDatum;

    function floor:IRethinkDBDatum;
    function ceil:IRethinkDBDatum;
    function round:IRethinkDBDatum;

    function concat(const v:OleVariant):IRethinkDBDatum;
    function slice_n1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBDatum;
    function slice_n(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBDatum;
    function count: IRethinkDBDatum;

    //IRethinkDBArray
    function filter_a(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBArray; overload;
    function filter_a(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBArray; overload;
    function innerJoin(const otherArray,predicate:IRethinkDBTerm):IRethinkDBArray;
    function outerJoin(const otherArray,predicate:IRethinkDBTerm):IRethinkDBArray;

    function append(const v:OleVariant):IRethinkDBDatum;
    function prepend(const v:OleVariant):IRethinkDBDatum;
    function difference(const v:OleVariant):IRethinkDBDatum;
    function zip:IRethinkDBArray;
    function map(const fn:IRethinkDBTerm):IRethinkDBArray; overload;
    function map(const arrays:array of IRethinkDBArray;const fn:IRethinkDBTerm):IRethinkDBArray; overload;
    function withFields(const selectors:array of WideString):IRethinkDBArray;
    function concatMap(const fn:IRethinkDBTerm):IRethinkDBArray; overload;
    function skip(n:integer):IRethinkDBArray;
    function limit(n:integer):IRethinkDBArray;
    function slice_a1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBArray;
    function slice_a(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBArray;
    function union_a1(const sequence:IRethinkDBSequence):IRethinkDBArray;
    function union_a2(const sequence:IRethinkDBSequence;const interleave:OleVariant):IRethinkDBArray;
    function union_a(const sequences:array of IRethinkDBSequence):IRethinkDBArray;
    function union_a3(const sequences:array of IRethinkDBSequence;const interleave:OleVariant):IRethinkDBArray;
    function sample_a(n:integer):IRethinkDBArray;

    //IRethinkDBSingleSelection
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function update(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function delete(const Options:IJSONDocument=nil):IRethinkDBObject;

  end;

  TRethinkDBBool=class(TRethinkDBDatum,IRethinkDBBool)
    function and_(const b:IRethinkDBBool):IRethinkDBBool;
    function or_(const b:IRethinkDBBool):IRethinkDBBool;
    function not_:IRethinkDBBool;
  end;

  TRethinkDBDatabase=class(TRethinkDBValue,IRethinkDBDatabase)
  protected
    function table(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBTable;

    function tableCreate(const TableName:WideString;
      const Options:IJSONDocument=nil):IRethinkDBObject;
    function tableDrop(const TableName:WideString):IRethinkDBObject;
    function tableList:IRethinkDBArray;
  end;

  TRethinkDBSet=class(TRethinkDBValue,IRethinkDBSequence,IRethinkDBStream,
    IRethinkDBSelection,IRethinkDBTable,IRethinkDBTableSlice
    )

    //IRethinkDBSequence
    function innerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBStream;
    function outerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBStream;
    function eqJoin(const leftFieldOrFunction,rightTable:IRethinkDBTerm;
      const Options:IJSONDocument=nil):IRethinkDBSequence;
    function map(const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function map(const sequences:array of IRethinkDBSequence;const fn:IRethinkDBTerm):IRethinkDBStream; overload;
    function withFields(const selectors:array of WideString):IRethinkDBStream;
    function orderBy_s1(const v:OleVariant):IRethinkDBArray;
    function orderBy_s(const vv:array of OleVariant):IRethinkDBArray;
    function skip(n:integer):IRethinkDBStream;
    function limit(n:integer):IRethinkDBStream;
    function nth_s(n:integer):IRethinkDBObject;
    function offsetsOf(DatumOrPredicate:OleVariant):IRethinkDBArray;
    function isEmpty:IRethinkDBBool;
    function sample_s(n:integer):IRethinkDBSelection;
    function group(const field:WideString;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function group(const func:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function reduce(const func:IRethinkDBTerm):IRethinkDBDatum;
    function fold(const base,func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function fold(const base,func,emit,finalEmit:IRethinkDBTerm):IRethinkDBSequence; overload;
    function sum(const field:WideString):IRethinkDBDatum; overload;
    function sum(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function avg(const field:WideString):IRethinkDBDatum; overload;
    function avg(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function min(const field:WideString):IRethinkDBDatum; overload;
    function min(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function min_index(const indexName:WideString):IRethinkDBDatum; overload;
    function max(const field:WideString):IRethinkDBDatum; overload;
    function max(const func:IRethinkDBTerm):IRethinkDBDatum; overload;
    function max_index(const indexName:WideString):IRethinkDBDatum; overload;
    function distinct_s:IRethinkDBArray;

    //IRethinkDBStream
    function changes(const Options:IJSONDocument=nil):IRethinkDBStream;
    function filter_s(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function filter_s(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBStream; overload;
    function zip:IRethinkDBStream;
    function concatMap(const fn:IRethinkDBTerm):IRethinkDBStream;
    function slice_z1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBStream;
    function slice_z(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBStream;
    function union_z1(const sequence:IRethinkDBSequence):IRethinkDBStream;
    function union_z2(const sequence:IRethinkDBSequence;const interleave:OleVariant):IRethinkDBStream;
    function union_z(const sequences:array of IRethinkDBSequence):IRethinkDBStream;
    function union_z3(const sequences:array of IRethinkDBSequence;const interleave:OleVariant):IRethinkDBStream;
    function sample_z(n:integer):IRethinkDBArray;
    function ungroup:IRethinkDBDatum;

    //IRethinkDBSelection
    function update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function update(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function replace(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function delete(const Options:IJSONDocument=nil):IRethinkDBObject;
    function sync:IRethinkDBObject;
    function filter_x(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBSelection; overload;
    function filter_x(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBSelection; overload;
    function orderBy_x1(const v:OleVariant):IRethinkDBSelection{<IRethinkDBArray>};
    function orderBy_x(const vv:array of OleVariant):IRethinkDBSelection{<IRethinkDBArray>};
    function slice_x1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBSelection;
    function slice_x(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
      rightBoundOpen:boolean=true):IRethinkDBSelection;
    function nth_x(n:integer):IRethinkDBSelection{<IRethinkDBObject>};

    //IRethinkDBTable
    function indexCreate(const IndexName:WideString;const IndexFunction:IRethinkDBTerm=nil;
      const Options:IJSONDocument=nil):IRethinkDBObject;
    function indexDrop(const IndexName:WideString):IRethinkDBObject;
    function indexList:IRethinkDBArray;
    function indexRename(const OldName,NewName:WideString):IRethinkDBObject;
    function indexStatus:IRethinkDBArray; overload;
    function indexStatus(const IndexNames:array of WideString):IRethinkDBTerm; overload;
    function indexWait:IRethinkDBArray; overload;
    function indexWait(const IndexNames:array of WideString):IRethinkDBTerm; overload;

    function insert(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;
    function insert(const docs:array of IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject; overload;

    function get(const Key:WideString):IRethinkDBSingleSelection;
    function getAll(const Keys:array of WideString;const Options:IJSONDocument=nil):IRethinkDBSelection;

    function between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;
    function orderBy_t1(const v:OleVariant):IRethinkDBTableSlice;
    function orderBy_t(const vv:array of OleVariant):IRethinkDBTableSlice; 

    function distinct_t(const indexName:WideString=''):IRethinkDBStream;

  end;

  TRethinkDBResultSet=class(TTHREADUNSAFEInterfacedObject,IRethinkDBResultSet)
  private
    FConnection:TRethinkDBConnection;
    FToken:int64;
  protected
    constructor Create(rdb:TRethinkDBConnection;token:int64);
  public
    function Get(const d:IJSONDocument):boolean;
    //TODO: next? each? toArray?
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

function StringEscape(const s:UTF8String): UTF8String;
var
  s1,s2:UTF8String;
  i1,i2,l1,l2:integer;
begin
  //Result:='"'+StringReplace(s,'"','\"',[rfReplaceAll])+'"';
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
  Result:=s2;
end;

type
  TRethinkDBFuncBody=class(TRethinkDBTerm,IRethinkDBTerm)
  private
    FData:IJSONDocument;
  protected
    procedure Build(b:TRethinkDBBuilder); override;
  public
    constructor Create(const Data:IJSONDocument);
  end;

{ TRethinkDB }

class function TRethinkDB.x(const s:WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBConstant.Create(StringEscape(s));
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
var
  DoFunc:boolean;
  e:IJSONEnumerator;
  t:IRethinkDBTerm;
begin
  DoFunc:=false;//default
  //ASSERT: either none or all values are IRethinkDBTerm, trigger on first
  e:=JSONEnum(d);
  if e.Next then
    if VarType(e.Value)=varUnknown then
      if IUnknown(e.Value).QueryInterface(IRethinkDBTerm,t)=S_OK then
        DoFunc:=true;//TODO: check all?
  if DoFunc then
    Result:=TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,x(1)),
      TRethinkDBFuncBody.Create(d)])
  else
    Result:=TRethinkDBValue.Create(TermType_MAKE_OBJ,nil,d);
end;

class function TRethinkDB.db(const DBName: WideString;
  const Options: IJSONDocument): IRethinkDBDatabase;
begin
  Result:=TRethinkDBDatabase.Create(TermType_DB,x(DBName),Options);
end;

class function TRethinkDB.table(const TableName: WideString;
  const Options: IJSONDocument): IRethinkDBTable;
begin
  Result:=TRethinkDBSet.Create(TermType_TABLE,x(TableName),Options) as IRethinkDBTable;
end;

class function TRethinkDB.dbCreate(const DBName: WideString): IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_DB_CREATE,x(DBName));
end;

class function TRethinkDB.dbDrop(const DBName: WideString): IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_DB_DROP,x(DBName)) as IRethinkDBObject;
end;

class function TRethinkDB.dbList: IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_DB_LIST,nil) as IRethinkDBArray;
end;

class function TRethinkDB.uuid(const Input: WideString): IRethinkDBDatum;
begin
  if Input='' then
    Result:=TRethinkDBDatum.Create(TermType_UUID,nil)
  else
    Result:=TRethinkDBDatum.Create(TermType_UUID,x(Input));
end;

class function TRethinkDB.http(const URL: WideString; const Options: IJSONDocument): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_HTTP,[x(URL)],Options);
end;


class function TRethinkDB.row: IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_IMPLICIT_VAR,nil);
end;

class function TRethinkDB.row(const FieldName: WideString): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_BRACKET,[
    //TRethinkDBDatum.Create(TermType_IMPLICIT_VAR,nil),
    TRethinkDBValue.Create(TermType_VAR,TRethinkDB.x(1)),
    x(FieldName)]);
end;

class function TRethinkDB.fn(const FnBody:IRethinkDBTerm;
  ArgCount:integer=1):IRethinkDBTerm;
var
  xx:array of IRethinkDBTerm;
  i:integer;
begin
  SetLength(xx,ArgCount);
  for i:=0 to ArgCount-1 do xx[i]:=x(i+1);
  Result:=TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,xx),
      FnBody]);
end;

class function TRethinkDB.fn(const FnBody:IJSONDocument;
  ArgCount:integer=1):IRethinkDBTerm;
var
  xx:array of IRethinkDBTerm;
  i:integer;
begin
  SetLength(xx,ArgCount);
  for i:=0 to ArgCount-1 do xx[i]:=x(i+1);
  Result:=TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,xx),
      TRethinkDBFuncBody.Create(FnBody)]);
end;

class function TRethinkDB.arg(const ArgIndex:integer):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_VAR,TRethinkDB.x(ArgIndex));
end;

class function TRethinkDB.arg(const ArgIndex:integer;
  const FieldName:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_BRACKET,[
    TRethinkDBValue.Create(TermType_VAR,TRethinkDB.x(ArgIndex)),
    x(FieldName)]);
end;

class function TRethinkDB.map(const sequences:array of IRethinkDBSequence;
  const fn:IRethinkDBTerm):IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  for i:=0 to l-1 do a[i]:=sequences[i];
  a[l]:=fn;
  Result:=TRethinkDBSet.Create(TermType_MAP,a) as IRethinkDBStream;
end;

class function TRethinkDB.map(const arrays:array of IRethinkDBArray;
  const fn:IRethinkDBTerm):IRethinkDBArray;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(arrays);
  SetLength(a,l+1);
  for i:=0 to l-1 do a[i]:=arrays[i];
  a[l]:=fn;
  Result:=TRethinkDBDatum.Create(TermType_MAP,a) as IRethinkDBArray;
end;

class function TRethinkDB.union(const stream:IRethinkDBStream;
  const sequences:array of IRethinkDBSequence):IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=stream;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBSet.Create(TermType_UNION,a) as IRethinkDBStream;
end;

class function TRethinkDB.union(const stream:IRethinkDBStream;
  const sequences:array of IRethinkDBSequence;
  const interleave:OleVariant):IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=stream;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBSet.Create(TermType_UNION,a,
    JSON(['interleave',interleave])) as IRethinkDBStream;
end;

class function TRethinkDB.union(const array_:IRethinkDBDatum;
  const sequences:array of IRethinkDBSequence):IRethinkDBDatum;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=array_;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBDatum.Create(TermType_UNION,a);
end;

class function TRethinkDB.union(const array_:IRethinkDBDatum;
  const sequences:array of IRethinkDBSequence;
  const interleave:OleVariant):IRethinkDBDatum;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=array_;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBDatum.Create(TermType_UNION,a,
    JSON(['interleave',interleave]));
end;

class function TRethinkDB.asc(const FieldName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_ASC,x(FieldName));
end;

class function TRethinkDB.desc(const FieldName: WideString): IRethinkDBTerm;
begin
  Result:=TRethinkDBValue.Create(TermType_DESC,x(FieldName));
end;

class function TRethinkDB.group(const sequence: IRethinkDBSequence;
  const field: WideString; const Options: IJSONDocument): IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_GROUP,[sequence,x(field)],Options) as IRethinkDBStream;
end;

class function TRethinkDB.group(const sequence: IRethinkDBSequence;
  const func: IRethinkDBTerm; const Options: IJSONDocument): IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_GROUP,[sequence,func],Options) as IRethinkDBStream;
end;

class function TRethinkDB.reduce(const sequence: IRethinkDBSequence;
  const func:IRethinkDBTerm): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_REDUCE,[sequence,func]);
end;

class function TRethinkDB.count(x:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_COUNT,x);
end;

class function TRethinkDB.sum(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_SUM,[sequence,x(field)]);
end;

class function TRethinkDB.sum(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_SUM,[sequence,func]);
end;

class function TRethinkDB.avg(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_AVG,[sequence,x(field)]);
end;

class function TRethinkDB.avg(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_AVG,[sequence,func]);
end;

class function TRethinkDB.min(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MIN,[sequence,x(field)]);
end;

class function TRethinkDB.min(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MIN,[sequence,func]);
end;

class function TRethinkDB.min_index(const sequence:IRethinkDBSequence;const indexName:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MIN,sequence,JSON(['index',indexName]));
end;

class function TRethinkDB.max(const sequence:IRethinkDBSequence;const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MAX,[sequence,x(field)]);
end;

class function TRethinkDB.max(const sequence:IRethinkDBSequence;const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MAX,[sequence,func]);
end;

class function TRethinkDB.max_index(const sequence:IRethinkDBSequence;const indexName:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MAX,sequence,JSON(['index',indexName]));
end;

class function TRethinkDB.distinct(const sequence:IRethinkDBSequence):IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_DISTINCT,[sequence]);
end;

class function TRethinkDB.distinct(const table:IRethinkDBTable;const indexName:WideString=''):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_DISTINCT,table,JSON(['index',indexName]));
end;

class function TRethinkDB.contains(const sequence:IRethinkDBSequence;const v:OleVariant):IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_CONTAINS,[sequence,xx(v)]);
end;

class function TRethinkDB.contains(const sequence:IRethinkDBSequence;const vv:array of OleVariant):IRethinkDBBool;
var
  x:array of IRethinkDBTerm;
  i,l:integer;
begin
  l:=Length(vv);
  SetLength(x,l);
  for i:=0 to l-1 do x[i]:=xx(vv[i]);
  Result:=TRethinkDBBool.Create(TermType_CONTAINS,x);
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
  e:IJSONEnumerator;
  bb:boolean;
  d:IJSONDocument;
  t:IRethinkDBTerm;
begin
  b('[');
  b(IntToStr(integer(FTermType)));
  if FFirstArg=nil then
   begin
    if FOptions<>nil then b(',[]');
   end
  else
   begin
    b(',[');
    t:=FFirstArg;
    while t<>nil do
     begin
      t.Build(b);
      t:=t.Next;
      if t=nil then b(']') else b(',');
     end;
   end;
  if FOptions<>nil then
   begin
    e:=JSONEnum(FOptions);
    bb:=true;
    b(',{');
    while e.Next do
     begin
      if bb then bb:=false else b(',');
      b(StringEscape(UTF8Encode(e.Key)));
      b(':');

      //see also TRethinkDB.xx //TODO: merge common bits

      case VarType(e.Value) of

        varEmpty,varNull:b('null');
        varSmallint,varInteger,varSingle,varDouble,varCurrency,
        $000E,//varDecimal
        varShortInt,varByte,varWord,varLongWord,varInt64,
        $0015://varWord64
          b(UTF8Encode(VarToWideStr(e.Value)));
        //varDate://TODO
        varOleStr:
          b(StringEscape(UTF8Encode(VarToWideStr(e.Value))));

        varDispatch,varUnknown:
          if IUnknown(e.Value).QueryInterface(IJSONDocument,d)=S_OK then
           begin
            t:=TRethinkDBValue.Create(TermType_MAKE_OBJ,nil,d);
            t.Build(b);
           end
          else
          if IUnknown(e.Value).QueryInterface(IRethinkDBTerm,t)=S_OK then
            t.Build(b)
          else
            raise ERethinkDBError.Create('Unsupported variant interface');

        //varError:?
        varBoolean:
          if boolean(e.Value) then b('true') else b('false');
        //varVariant:?

        //varStrArg //?

        //varTypeMask = $0FFF;
        //varArray    = $2000;
        //varByRef    = $4000;

        else raise ERethinkDBError.Create('Unsupported variant type #'+IntToHex(VarType(e.Value),4));
      end;
     end;
    b('}');
   end;
  b(']');
end;

function TRethinkDBValue.PrepOrderBy(rt:TRethinkDBValueClass;
  const vv:array of OleVariant):IRethinkDBTerm;
var
  i,j,l:integer;
  a:array of IRethinkDBTerm;
  u:IUnknown;
  d,d1:IJSONDocument;
  t:IRethinkDBTerm;
begin
  l:=Length(vv);
  SetLength(a,l+1);
  a[0]:=Self;
  d1:=nil;
  j:=1;
  for i:=0 to l-1 do
    case VarType(vv[i]) of
      varDispatch,varUnknown:
       begin
        u:=IUnknown(vv[i]);
        if u.QueryInterface(IJSONDocument,d)=S_OK then
         begin
          if d1<>nil then
            raise ERethinkDBError.Create('orderBy allows only one {index:""} argument');
          //TODO: check not(VarIsNull(d['index']))?
          d1:=d;
         end
        else
        if u.QueryInterface(IRethinkDBTerm,t)=S_OK then
         begin
          a[j]:=t;
          inc(j);
         end
        else
          raise ERethinkDBError.Create('Unsupported orderBy argument type at #'+IntToStr(i));
       end;
      varOleStr:
       begin
        a[j]:=TRethinkDB.x(VarToWideStr(vv[i]));
        inc(j);
       end;
      else
        raise ERethinkDBError.Create('Unsupported orderBy argument type at #'+IntToStr(i));
    end;
  SetLength(a,j);
  Result:=rt.Create(TermType_ORDER_BY,a,d1);
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

function TRethinkDBDatum.add_(const v: OleVariant): IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_ADD,[Self,r.xx(v)]);
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
  Result:=TRethinkDBBool.Create(TermType_ADD,[Self,r.xx(v)]);//yes it's "ADD", see ql2.proto
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

function TRethinkDBDatum.innerJoin(const otherArray, predicate: IRethinkDBTerm): IRethinkDBArray;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBDatum.Create(TermType_INNER_JOIN,[Self,otherArray,predicate]);
end;

function TRethinkDBDatum.outerJoin(const otherArray, predicate: IRethinkDBTerm): IRethinkDBArray;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBDatum.Create(TermType_INNER_JOIN,[Self,otherArray,predicate]);
end;

function TRethinkDBDatum.zip: IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_Zip,[Self]) as IRethinkDBArray;
end;

function TRethinkDBDatum.map(const fn: IRethinkDBTerm): IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_MAP,[Self,fn]) as IRethinkDBArray;
end;

function TRethinkDBDatum.map(const arrays: array of IRethinkDBArray;
  const fn: IRethinkDBTerm): IRethinkDBArray;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(arrays);
  SetLength(a,l+2);
  a[0]:=Self;
  for i:=0 to l-1 do a[i+1]:=arrays[i];
  a[l+1]:=fn;
  Result:=TRethinkDBDatum.Create(TermType_MAP,a) as IRethinkDBArray;
end;

function TRethinkDBDatum.withFields(const selectors: array of WideString): IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_WITH_FIELDS,r.xa(Self,selectors)) as IRethinkDBArray;
end;

function TRethinkDBDatum.concatMap(const fn: IRethinkDBTerm): IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_CONCAT_MAP,[Self,fn]);
end;

function TRethinkDBDatum.skip(n: integer): IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_SKIP,[Self,r.x(n)]);
end;

function TRethinkDBDatum.limit(n: integer): IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_LIMIT,[Self,r.x(n)]);
end;

function TRethinkDBDatum.count: IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_COUNT,Self);
end;

function TRethinkDBDatum.slice_n1(startOffset:cardinal;
  leftBoundOpen:boolean=false):IRethinkDBDatum;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  Result:=TRethinkDBDatum.Create(TermType_SLICE,[Self,r.x(startOffset)],d);
end;

function TRethinkDBDatum.slice_n(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
  rightBoundOpen:boolean=true):IRethinkDBDatum;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  if not(rightBoundOpen) then
   begin
    if d=nil then d:=JSON;
    d['rightBound']:='closed';
   end;
  Result:=TRethinkDBDatum.Create(TermType_SLICE,[Self,r.x(startOffset),r.x(endOffset)],d);
end;

function TRethinkDBDatum.filter_a(const KeyValue:IJSONDocument;
  const Options:IJSONDocument=nil):IRethinkDBArray;
var
  a,b:IRethinkDBTerm;
  e:IJSONEnumerator;
begin
  a:=nil;
  e:=JSONEnum(KeyValue);
  while e.Next do
   begin
    b:=r.row(e.Key).eq(e.Value);
    if a=nil then a:=b else a:=TRethinkDBValue.Create(TermType_AND,[a,b]);
   end;
  Result:=TRethinkDBDatum.Create(TermType_FILTER,[Self,a],Options);
end;

function TRethinkDBDatum.filter_a(const Predicate:IRethinkDBTerm;
  const Options:IJSONDocument=nil):IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_FILTER,[Self,Predicate],Options);
end;

function TRethinkDBDatum.slice_a1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBArray;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  Result:=TRethinkDBDatum.Create(TermType_SLICE,[Self,r.x(startOffset)],d);
end;

function TRethinkDBDatum.slice_a(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
  rightBoundOpen:boolean=true):IRethinkDBArray;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  if not(rightBoundOpen) then
   begin
    if d=nil then d:=JSON;
    d['rightBound']:='closed';
   end;
  Result:=TRethinkDBDatum.Create(TermType_SLICE,[Self,r.x(startOffset),r.x(endOffset)],d);
end;

function TRethinkDBDatum.union_a1(const sequence:IRethinkDBSequence):IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_UNION,[Self,sequence]);
end;

function TRethinkDBDatum.union_a2(const sequence:IRethinkDBSequence;const interleave:OleVariant):IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_UNION,[Self,sequence],JSON(['interleave',interleave]));
end;

function TRethinkDBDatum.union_a(const sequences:array of IRethinkDBSequence):IRethinkDBArray;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=Self;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBDatum.Create(TermType_UNION,a);
end;

function TRethinkDBDatum.union_a3(const sequences:array of IRethinkDBSequence;const interleave:OleVariant):IRethinkDBArray;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=Self;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBDatum.Create(TermType_UNION,a,JSON(['interleave',interleave]));
end;

function TRethinkDBDatum.sample_a(n:integer):IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_SAMPLE,[Self,r.x(n)]);
end;

function TRethinkDBDatum.update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_UPDATE,[Self,r.x(doc)],Options);
end;

function TRethinkDBDatum.update(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_UPDATE,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),fn])
    ],Options);
end;

function TRethinkDBDatum.replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_REPLACE,[Self,r.x(doc)],Options);
end;

function TRethinkDBDatum.replace(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_UPDATE,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),fn])
    ],Options);
end;

function TRethinkDBDatum.delete(const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_DELETE,Self,Options);
end;

{ TRethinkDBBool }

function TRethinkDBBool.and_(const b:IRethinkDBBool):IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_AND,[Self,b]);
end;

function TRethinkDBBool.or_(const b:IRethinkDBBool):IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_OR,[Self,b]);
end;

function TRethinkDBBool.not_: IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_NOT,Self);
end;

{ TRethinkDBConstant }

constructor TRethinkDBFuncBody.Create(const Data: IJSONDocument);
begin
  inherited Create;
  FData:=Data;
end;

procedure TRethinkDBFuncBody.Build(b: TRethinkDBBuilder);
var
  e:IJSONEnumerator;
  t:IRethinkDBTerm;
  bb:boolean;
begin
  e:=JSONEnum(FData);
  bb:=true;
  b('{');
  while e.Next do
   begin
    if bb then bb:=false else b(',');
    b(StringEscape(UTF8Encode(e.Key)));
    b(':');
    if (VarType(e.Value)=varUnknown) and
      (IUnknown(e.Value).QueryInterface(IRethinkDBTerm,t)=S_OK) then
      t.Build(b)
    else
      raise ERethinkDBError.Create('Can''t mix terms and values in a document.');
   end;
  b('}');
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

{ TRethinkDBDatabase }

function TRethinkDBDatabase.table(const TableName: WideString; const Options: IJSONDocument): IRethinkDBTable;
begin
  Result:=TRethinkDBSet.Create(TermType_TABLE,[Self,r.x(TableName)],Options) as IRethinkDBTable;
end;

function TRethinkDBDatabase.tableCreate(const TableName: WideString;
  const Options: IJSONDocument): IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_TABLE_CREATE,[Self,r.x(TableName)],Options) as IRethinkDBObject;
end;

function TRethinkDBDatabase.tableDrop(const TableName: WideString): IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_TABLE_DROP,[Self,r.x(TableName)]);
end;

function TRethinkDBDatabase.tableList: IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_TABLE_LIST,Self);
end;

{ TRethinkDBSet }

function TRethinkDBSet.innerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBStream;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBSet.Create(TermType_INNER_JOIN,[Self,otherSequence,predicate]);
end;

function TRethinkDBSet.outerJoin(const otherSequence,predicate:IRethinkDBTerm):IRethinkDBStream;
begin
  //TODO: check predicate 2 parameters?
  Result:=TRethinkDBSet.Create(TermType_OUTER_JOIN,[Self,otherSequence,predicate]);
end;

function TRethinkDBSet.eqJoin(const leftFieldOrFunction,rightTable:IRethinkDBTerm;
  const Options:IJSONDocument=nil):IRethinkDBSequence;
begin
  Result:=TRethinkDBSet.Create(TermType_EQ_JOIN,[Self,leftFieldOrFunction,rightTable],Options);
end;

function TRethinkDBSet.map(const fn:IRethinkDBTerm):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_MAP,[Self,fn]);
end;

function TRethinkDBSet.map(const sequences:array of IRethinkDBSequence;const fn:IRethinkDBTerm):IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+2);
  a[0]:=Self;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  a[l+1]:=fn;
  Result:=TRethinkDBSet.Create(TermType_MAP,a);
end;

function TRethinkDBSet.withFields(const selectors:array of WideString):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_WITH_FIELDS,r.xa(Self,selectors));
end;

function TRethinkDBSet.orderBy_s1(const v:OleVariant):IRethinkDBArray;
begin
  Result:=PrepOrderBy(TRethinkDBDatum,[v]) as IRethinkDBArray;
end;

function TRethinkDBSet.orderBy_s(const vv:array of OleVariant):IRethinkDBArray;
begin
  Result:=PrepOrderBy(TRethinkDBDatum,vv) as IRethinkDBArray;
end;

function TRethinkDBSet.skip(n:integer):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_SKIP,[Self,r.x(n)]);
end;

function TRethinkDBSet.limit(n:integer):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_LIMIT,[Self,r.x(n)]);
end;

function TRethinkDBSet.nth_s(n:integer):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_NTH,[Self,r.x(n)]);
end;

function TRethinkDBSet.offsetsOf(DatumOrPredicate:OleVariant):IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_OFFSETS_OF,[Self,r.xx(DatumOrPredicate)]);
end;

function TRethinkDBSet.isEmpty:IRethinkDBBool;
begin
  Result:=TRethinkDBBool.Create(TermType_IS_EMPTY,Self);
end;

function TRethinkDBSet.sample_s(n:integer):IRethinkDBSelection;
begin
  Result:=TRethinkDBSet.Create(TermType_SAMPLE,[Self,r.x(n)]);
end;

function TRethinkDBSet.group(const field:WideString;const Options:IJSONDocument=nil):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_GROUP,[Self,r.x(field)],Options);
end;

function TRethinkDBSet.group(const func:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_GROUP,[Self,func],Options);
end;

function TRethinkDBSet.reduce(const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_REDUCE,[Self,func]);
end;

function TRethinkDBSet.fold(const base,func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_FOLD,[Self,base,func]);
end;

function TRethinkDBSet.fold(const base,func,emit,finalEmit:IRethinkDBTerm):IRethinkDBSequence;
begin
  Result:=TRethinkDBSet.Create(TermType_FOLD,[Self,base,func],
    JSON(['emit',emit,'finalEmit',finalEmit]));
end;

function TRethinkDBSet.sum(const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_SUM,[Self,r.x(field)]);
end;

function TRethinkDBSet.sum(const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_SUM,[Self,func]);
end;

function TRethinkDBSet.avg(const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_AVG,[Self,r.x(field)]);
end;

function TRethinkDBSet.avg(const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_AVG,[Self,func]);
end;

function TRethinkDBSet.min(const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MIN,[Self,r.x(field)]);
end;

function TRethinkDBSet.min(const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MIN,[Self,func]);
end;

function TRethinkDBSet.min_index(const indexName:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MIN,Self,JSON(['index',indexName]));
end;

function TRethinkDBSet.max(const field:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MAX,[Self,r.x(field)]);
end;

function TRethinkDBSet.max(const func:IRethinkDBTerm):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MAX,[Self,func]);
end;

function TRethinkDBSet.max_index(const indexName:WideString):IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_MAX,Self,JSON(['index',indexName]));
end;

function TRethinkDBSet.distinct_s:IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_DISTINCT,[Self]);
end;

function TRethinkDBSet.changes(const Options:IJSONDocument=nil):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_CHANGES,Self,Options);
end;

function TRethinkDBSet.filter_s(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBStream;
var
  a,b:IRethinkDBTerm;
  e:IJSONEnumerator;
begin
  a:=nil;
  e:=JSONEnum(KeyValue);
  while e.Next do
   begin
    b:=r.row(e.Key).eq(e.Value);
    if a=nil then a:=b else a:=TRethinkDBValue.Create(TermType_AND,[a,b]);
   end;
  Result:=TRethinkDBSet.Create(TermType_FILTER,[Self,a],Options);
end;

function TRethinkDBSet.filter_s(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_FILTER,[Self,Predicate],Options);
end;

function TRethinkDBSet.zip:IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_ZIP,[Self]);
end;

function TRethinkDBSet.concatMap(const fn:IRethinkDBTerm):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_CONCAT_MAP,[Self,fn]);
end;

function TRethinkDBSet.slice_z1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBStream;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  Result:=TRethinkDBSet.Create(TermType_SLICE,[Self,r.x(startOffset)],d);
end;

function TRethinkDBSet.slice_z(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
  rightBoundOpen:boolean=true):IRethinkDBStream;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  if not(rightBoundOpen) then
   begin
    if d=nil then d:=JSON;
    d['rightBound']:='closed';
   end;
  Result:=TRethinkDBSet.Create(TermType_SLICE,[Self,r.x(startOffset),r.x(endOffset)],d);
end;

function TRethinkDBSet.union_z1(const sequence:IRethinkDBSequence):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_UNION,[Self,sequence]);
end;

function TRethinkDBSet.union_z2(const sequence:IRethinkDBSequence;const interleave:OleVariant):IRethinkDBStream;
begin
  Result:=TRethinkDBSet.Create(TermType_UNION,[Self,sequence],JSON(['interleave',interleave]));
end;

function TRethinkDBSet.union_z(const sequences:array of IRethinkDBSequence):IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=Self;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBSet.Create(TermType_UNION,a);
end;

function TRethinkDBSet.union_z3(const sequences:array of IRethinkDBSequence;const interleave:OleVariant):IRethinkDBStream;
var
  i,l:integer;
  a:array of IRethinkDBTerm;
begin
  l:=Length(sequences);
  SetLength(a,l+1);
  a[0]:=Self;
  for i:=0 to l-1 do a[i+1]:=sequences[i];
  Result:=TRethinkDBSet.Create(TermType_UNION,a,JSON(['interleave',interleave]));
end;

function TRethinkDBSet.sample_z(n:integer):IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_SAMPLE,[Self,r.x(n)]);
end;

function TRethinkDBSet.ungroup:IRethinkDBDatum;
begin
  Result:=TRethinkDBDatum.Create(TermType_UNGROUP,[Self]);
end;

function TRethinkDBSet.update(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_UPDATE,[Self,r.x(doc)],Options);
end;

function TRethinkDBSet.update(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_UPDATE,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),fn])
    ],Options);
end;

function TRethinkDBSet.replace(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_REPLACE,[Self,r.x(doc)],Options);
end;

function TRethinkDBSet.replace(const fn:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_UPDATE,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),fn])
    ],Options);
end;

function TRethinkDBSet.delete(const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_DELETE,Self,Options);
end;

function TRethinkDBSet.sync:IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_SYNC,Self);
end;

function TRethinkDBSet.filter_x(const KeyValue:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBSelection;
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
  Result:=TRethinkDBSet.Create(TermType_FILTER,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),a])],Options);
end;

function TRethinkDBSet.filter_x(const Predicate:IRethinkDBTerm;const Options:IJSONDocument=nil):IRethinkDBSelection;
begin
  Result:=TRethinkDBSet.Create(TermType_Filter,[Self,
    TRethinkDBValue.Create(TermType_FUNC,[
      TRethinkDBValue.Create(TermType_MAKE_ARRAY,r.x(1)),Predicate])],Options);
end;

function TRethinkDBSet.orderBy_x1(const v:OleVariant):IRethinkDBSelection{<IRethinkDBArray>};
begin
  Result:=PrepOrderBy(TRethinkDBSet,[v]) as IRethinkDBSelection;
end;

function TRethinkDBSet.orderBy_x(const vv:array of OleVariant):IRethinkDBSelection{<IRethinkDBArray>};
begin
  Result:=PrepOrderBy(TRethinkDBSet,vv) as IRethinkDBSelection;
end;

function TRethinkDBSet.slice_x1(startOffset:cardinal;leftBoundOpen:boolean=false):IRethinkDBSelection;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  Result:=TRethinkDBSet.Create(TermType_SLICE,[Self,r.x(startOffset)],d);
end;

function TRethinkDBSet.slice_x(startOffset,endOffset:cardinal;leftBoundOpen:boolean=false;
  rightBoundOpen:boolean=true):IRethinkDBSelection;
var
  d:IJSONDocument;
begin
  if leftBoundOpen then d:=JSON(['leftBound','open']) else d:=nil;
  if not(rightBoundOpen) then
   begin
    if d=nil then d:=JSON;
    d['rightBound']:='closed';
   end;
  Result:=TRethinkDBSet.Create(TermType_SLICE,[Self,r.x(startOffset),r.x(endOffset)],d);
end;

function TRethinkDBSet.nth_x(n:integer):IRethinkDBSelection{<IRethinkDBObject>};
begin
  Result:=TRethinkDBSet.Create(TermType_NTH,[Self,r.x(n)]);
end;

function TRethinkDBSet.indexCreate(const IndexName:WideString;const IndexFunction:IRethinkDBTerm=nil;
  const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_CREATE,[Self,r.x(IndexName),IndexFunction],Options);
end;

function TRethinkDBSet.indexDrop(const IndexName:WideString):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_DROP,[Self,r.x(IndexName)]);
end;

function TRethinkDBSet.indexList:IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_LIST,Self);
end;

function TRethinkDBSet.indexRename(const OldName,NewName:WideString):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_RENAME,[Self,r.x(OldName),r.x(NewName)]);
end;

function TRethinkDBSet.indexStatus:IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_STATUS,Self);
end;

function TRethinkDBSet.indexStatus(const IndexNames:array of WideString):IRethinkDBTerm;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_STATUS,r.xa(Self,IndexNames));
end;

function TRethinkDBSet.indexWait:IRethinkDBArray;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_WAIT,Self);
end;

function TRethinkDBSet.indexWait(const IndexNames:array of WideString):IRethinkDBTerm;
begin
  Result:=TRethinkDBDatum.Create(TermType_INDEX_WAIT,r.xa(Self,IndexNames));
end;

function TRethinkDBSet.insert(const doc:IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject;
begin
  Result:=TRethinkDBDatum.Create(TermType_INSERT,[Self,r.x(doc)],Options);
end;

function TRethinkDBSet.insert(const docs:array of IJSONDocument;const Options:IJSONDocument=nil):IRethinkDBObject;
var
  a:array of IRethinkDBTerm;
  i,l:integer;
begin
  l:=Length(docs);
  SetLength(a,l);
  for i:=0 to l-1 do a[i]:=r.x(docs[i]);
  Result:=TRethinkDBDatum.Create(TermType_INSERT,[Self,
    TRethinkDBValue.Create(TermType_MAKE_ARRAY,a)],Options);
end;

function TRethinkDBSet.get(const Key:WideString):IRethinkDBSingleSelection;
begin
  Result:=TRethinkDBDatum.Create(TermType_GET,[Self,r.x(Key)]);
end;

function TRethinkDBSet.getAll(const Keys:array of WideString;const Options:IJSONDocument=nil):IRethinkDBSelection;
begin
  Result:=TRethinkDBSet.Create(TermType_GET_ALL,r.xa(Self,Keys),Options);
end;

function TRethinkDBSet.between(const LowerKey,UpperKey:WideString;const Options:IJSONDocument=nil):IRethinkDBTableSlice;
begin
  Result:=TRethinkDBSet.Create(TermType_BETWEEN,[Self,r.x(LowerKey),r.x(UpperKey)],Options);
end;

function TRethinkDBSet.orderBy_t1(const v:OleVariant):IRethinkDBTableSlice;
begin
  Result:=PrepOrderBy(TRethinkDBSet,[v]) as IRethinkDBTableSlice;
end;

function TRethinkDBSet.orderBy_t(const vv:array of OleVariant):IRethinkDBTableSlice;
begin
  Result:=PrepOrderBy(TRethinkDBSet,vv) as IRethinkDBTableSlice;
end;

function TRethinkDBSet.distinct_t(const indexName:WideString=''):IRethinkDBStream;
var
  d:IJSONDocument;
begin
  if indexName='' then d:=nil else d:=JSON(['index',indexName]);
  Result:=TRethinkDBSet.Create(TermType_DISTINCT,Self,d);
end;

end.
