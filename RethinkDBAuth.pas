unit RethinkDBAuth;

{$D-}
{$L-}

interface

uses RethinkDB, jsonDoc;

type
  RethinkDBAuthenticateExchange=function(const d:IJSONDocument):IJSONDocument of object;

procedure RethinkDBAuthenticate(RethinkDBEx: RethinkDBAuthenticateExchange;
  ProtocolVersion: integer; const UserName, Password: WideString);

type
  ERethinkDBAuthenticationFailed=class(ERethinkDBError);

implementation

uses SysUtils;

{$IF not Declared(RawByteString)}
type
  RawByteString=AnsiString;
{$IFEND}

{
function SwapEndian32(Value: cardinal): cardinal; register; overload;
asm
  bswap eax
end;
}

function SwapEndian32(Value: cardinal): cardinal;
var
  x:array[0..3] of byte absolute Result;
  y:array[0..3] of byte absolute Value;
begin
  x[0]:=y[3];
  x[1]:=y[2];
  x[2]:=y[1];
  x[3]:=y[0];
end;

function SHA256Hash(x: RawByteString): RawByteString;
const
  base:array[0..63] of cardinal=(
    $428a2f98, $71374491, $b5c0fbcf, $e9b5dba5,
    $3956c25b, $59f111f1, $923f82a4, $ab1c5ed5,
    $d807aa98, $12835b01, $243185be, $550c7dc3,
    $72be5d74, $80deb1fe, $9bdc06a7, $c19bf174,
    $e49b69c1, $efbe4786, $0fc19dc6, $240ca1cc,
    $2de92c6f, $4a7484aa, $5cb0a9dc, $76f988da,
    $983e5152, $a831c66d, $b00327c8, $bf597fc7,
    $c6e00bf3, $d5a79147, $06ca6351, $14292967,
    $27b70a85, $2e1b2138, $4d2c6dfc, $53380d13,
    $650a7354, $766a0abb, $81c2c92e, $92722c85,
    $a2bfe8a1, $a81a664b, $c24b8b70, $c76c51a3,
    $d192e819, $d6990624, $f40e3585, $106aa070,
    $19a4c116, $1e376c08, $2748774c, $34b0bcb5,
    $391c0cb3, $4ed8aa4a, $5b9cca4f, $682e6ff3,
    $748f82ee, $78a5636f, $84c87814, $8cc70208,
    $90befffa, $a4506ceb, $bef9a3f7, $c67178f2);
  hex:array[0..15] of AnsiChar='0123456789abcdef';
var
  a,b:cardinal;
  dl,i,j:integer;
  d:array of cardinal;
  e:array[0..63] of cardinal;
  g,h:array[0..7] of cardinal;
begin
  a:=Length(x);
  dl:=a+9;
  if (dl mod 64)<>0 then dl:=((dl div 64)+1)*64;
  i:=dl;
  dl:=dl div 4;
  SetLength(d,dl);
  SetLength(x,i);
  j:=a+1;
  x[j]:=#$80;
  while j<i do
   begin
    inc(j);
    x[j]:=#0;
   end;
  Move(x[1],d[0],i);
  d[dl-1]:=SwapEndian32(a shl 3);
  h[0]:=$6a09e667;
  h[1]:=$bb67ae85;
  h[2]:=$3c6ef372;
  h[3]:=$a54ff53a;
  h[4]:=$510e527f;
  h[5]:=$9b05688c;
  h[6]:=$1f83d9ab;
  h[7]:=$5be0cd19;
  i:=0;
  while i<dl do
   begin
    j:=0;
    while j<16 do
     begin
      e[j]:=SwapEndian32(d[i]);
      inc(i);
      inc(j);
     end;
    while j<64 do
     begin
      a:=e[j-15];
      b:=e[j-2];
      e[j]:=e[j-16]+
        (((a shr  7) or (a shl 25)) xor
         ((a shr 18) or (a shl 14)) xor
          (a shr  3))+
        e[j-7]+
        (((b shr 17) or (b shl 15)) xor
         ((b shr 19) or (b shl 13)) xor
          (b shr 10));
      inc(j);
     end;
    g:=h;
    j:=0;
    while j<64 do
     begin
      a:=g[4];
      b:=g[0];
      a:=g[7]+
        (((a shr  6) or (a shl 26)) xor
         ((a shr 11) or (a shl 21)) xor
         ((a shr 25) or (a shl  7)))+
        ((g[4] and g[5]) or (not(g[4]) and g[6]))+
        base[j]+e[j];
      inc(g[3],a);
      a:=a+
        (((b shr  2) or (b shl 30)) xor
         ((b shr 13) or (b shl 19)) xor
         ((b shr 22) or (b shl 10)))+
        ((g[0] and g[1]) or (g[1] and g[2]) or (g[2] and g[0]));
      g[7]:=g[6];
      g[6]:=g[5];
      g[5]:=g[4];
      g[4]:=g[3];
      g[3]:=g[2];
      g[2]:=g[1];
      g[1]:=g[0];
      g[0]:=a;
      inc(j);
     end;
    for j:=0 to 7 do inc(h[j],g[j]);
   end;
  SetLength(Result,32);
  for j:=0 to 31 do
    Result[j+1]:=AnsiChar(h[j shr 2] shr ((3-(j and 3))*8));
end;

function HMAC_SHA256(const Key, Msg: RawByteString): RawByteString;
const
  BlockSize=64;//SHA256
var
  k,h1,h2:UTF8String;
  i:integer;
begin
  //assert BlockSize=Length(HashFn(''))
  if Length(Key)>BlockSize then k:=SHA256Hash(Key) else
   begin
    k:=Key;
    i:=Length(k);
    SetLength(k,BlockSize);
    while (i<BlockSize) do
     begin
      inc(i);
      k[i]:=#0;
     end;
   end;
  SetLength(h1,BlockSize);
  SetLength(h2,BlockSize);
  //TODO: speed-up by doing 32 bits at a time
  for i:=1 to BlockSize do byte(h1[i]):=byte(k[i]) xor $5C;
  for i:=1 to BlockSize do byte(h2[i]):=byte(k[i]) xor $36;
  Result:=SHA256Hash(h1+SHA256Hash(h2+Msg));
end;

function PBKDF2_HMAC_SHA256(const Password, Salt: RawByteString;
  Iterations, KeyLength: cardinal): RawByteString;
var
  i,j,k,c,l:cardinal;
  x,y:UTF8String;
const
  HashLength=32;//Length(SHA256Hash())
begin
  //assert HashLength:=Length(PRF('',''))
  l:=KeyLength div HashLength;
  if (KeyLength mod HashLength)<>0 then inc(l);
  SetLength(Result,l*HashLength);
  i:=0;
  j:=0;
  while (i<KeyLength) do
   begin
    inc(j);
    x:=HMAC_SHA256(Password,Salt+
      AnsiChar(j shr 24)+AnsiChar((j shr 16) and $FF)+
      AnsiChar((j shr 8) and $FF)+AnsiChar(j and $FF));
    y:=x;
    c:=Iterations-1;
    while c<>0 do
     begin
      x:=HMAC_SHA256(Password,x);
      for k:=1 to Length(x) do
        byte(y[k]):=byte(y[k]) xor byte(x[k]);
      dec(c);
     end;
    for k:=1 to HashLength do
     begin
      inc(i);
      Result[i]:=y[k];
     end;
   end;
  SetLength(Result,KeyLength);
end;

const
  HexCodes:array[0..15] of AnsiChar='0123456789abcdef';

function HexEncode(const x: RawByteString): UTF8string;
var
  i,l:integer;
begin
  l:=Length(x);
  SetLength(Result,l*2);
  i:=0;
  while i<l do
   begin
    inc(i);
    Result[i*2-1]:=HexCodes[byte(x[i]) shr $4];
    Result[i*2  ]:=HexCodes[byte(x[i]) and $F];
   end;
end;

const
  Base64Codes:array[0..63] of AnsiChar=
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

function Base64Encode(const x: RawByteString): UTF8String;
var
  i,j,l:cardinal;
begin
  l:=Length(x);
  i:=(l div 3);
  if (l mod 3)<>0 then inc(i);
  SetLength(Result,i*4);
  i:=1;
  j:=0;
  while (i+2<=l) do
   begin
    inc(j);Result[j]:=Base64Codes[  byte(x[i  ]) shr 2];
    inc(j);Result[j]:=Base64Codes[((byte(x[i  ]) and $03) shl 4)
                                or (byte(x[i+1]) shr 4)];
    inc(j);Result[j]:=Base64Codes[((byte(x[i+1]) and $0F) shl 2)
                                or (byte(x[i+2]) shr 6)];
    inc(j);Result[j]:=Base64Codes[  byte(x[i+2]) and $3F];
    inc(i,3);
   end;
  if i=l then
   begin
    inc(j);Result[j]:=Base64Codes[  byte(x[i  ]) shr 2];
    inc(j);Result[j]:=Base64Codes[((byte(x[i  ]) and $03) shl 4)];
    inc(j);Result[j]:='=';
    inc(j);Result[j]:='=';
   end
  else if i+1=l then
   begin
    inc(j);Result[j]:=Base64Codes[  byte(x[i  ]) shr 2];
    inc(j);Result[j]:=Base64Codes[((byte(x[i  ]) and $03) shl 4)
                                or (byte(x[i+1]) shr 4)];
    inc(j);Result[j]:=Base64Codes[((byte(x[i+1]) and $0F) shl 2)];
    inc(j);Result[j]:='=';
   end;
end;

function Base64Decode(const x: UTF8String): RawByteString;
var
  i,j,k,l,m:cardinal;
  n:array[0..3] of byte;
begin
  l:=Length(x);
  if l<4 then Result:='' else
   begin
    k:=(Length(x) div 4)*3;
    SetLength(Result,k);
    if x[l]='=' then begin dec(k); dec(l); end;
    if x[l]='=' then begin dec(k); dec(l); end;
    i:=0;
    j:=0;
    while i<l do
     begin
      m:=0;
      while m<>4 do
       begin
        if i=l then n[m]:=0 else
         begin
          inc(i);
          n[m]:=0;
          while (n[m]<64) and (x[i]<>Base64Codes[n[m]]) do inc(n[m]);
         end;
        inc(m);
       end;
      inc(j);Result[j]:=AnsiChar((n[0] shl 2) or (n[1] shr 4));
      inc(j);Result[j]:=AnsiChar((n[1] shl 4) or (n[2] shr 2));
      inc(j);Result[j]:=AnsiChar((n[2] shl 6) or (n[3]      ));
     end;
    SetLength(Result,k);
   end;
end;

function BuildNonce: RawByteString;
var
  x:packed record g1,g2:TGUID; end;
begin
  if CreateGUID(x.g1)<>S_OK then RaiseLastOSError;
  if CreateGUID(x.g2)<>S_OK then RaiseLastOSError;
  SetLength(Result,24);
  Move(x,Result[1],24);
end;

procedure RethinkDBAuthenticate(RethinkDBEx: RethinkDBAuthenticateExchange;
  ProtocolVersion: integer; const UserName, Password: WideString);
var
  nonce,m1,m2,m3,m4,r,s,t,u:Utf8String;
  i,j,k,l:integer;
const
  ErrMsg='RethinkDB: failed to authenticate as "%s"';
begin
  //https://tools.ietf.org/html/rfc5802

  //TODO: unsername, password: unicode normalize 'form KC'
  //TODO: strprep https://tools.ietf.org/html/rfc4013
  nonce:=Base64Encode(BuildNonce);

  //client-first message
  m1:='n='+UTF8Encode(
    StringReplace(
    StringReplace(
      UserName
      ,'=','=3D',[rfReplaceAll])
      ,',','=2C',[rfReplaceAll])
      )+',r='+nonce;
  t:='n,,'+m1;

  m2:=RethinkDBEx(JSON(
    ['protocol_version',ProtocolVersion
    ,'authentication_method','SCRAM-SHA-256'
    ,'authentication',t
    ]))['authentication'];

  r:='';//default
  s:='';//default
  k:=0;//default
  l:=Length(m2);
  i:=1;
  while (i<=l) do
    if m2[i+1]<>'=' then i:=l else
     begin
      j:=i+2;
      while (j<l) and (m2[j]<>',') do inc(j);
      //assert m2[i]='=';
      case m2[i] of
        'r'://combined nonces
          r:=Copy(m2,i+2,j-i-2);
        's'://salt
          s:=Copy(m2,i+2,j-i-2);
        'i'://iteration count
         begin
          //StrToInt?
          k:=0;
          inc(i);//'='
          while i<j do
           begin
            inc(i);
            k:=k*10+(byte(m2[i]) and $F);
           end;
         end;
        //else raise?
      end;
      i:=j+1;
     end;

  if (r='') or (s='') or (k=0) then
    raise ERethinkDBAuthenticationFailed.CreateFmt(ErrMsg,[UserName]);
  if Copy(r,1,Length(nonce))<>nonce then
    raise ERethinkDBAuthenticationFailed.CreateFmt(ErrMsg,[UserName]);
  //TODO: store server nonce, enforce unicity

  //calculate client proof
  m3:=//'c=biws'//
  'c='+Base64Encode('n,,')
    +',r='+r;
  s:=PBKDF2_HMAC_SHA256(Password,Base64Decode(s),k,32);
  t:=HMAC_SHA256(s,'Client Key');
  u:=m1+','+m2+','+m3;
  r:=HMAC_SHA256(SHA256Hash(t),u);
  for i:=1 to Length(t) do byte(t[i]):=byte(t[i]) xor byte(r[i]);


  //client-final message
  m3:=m3+',p='+Base64Encode(t);

  t:=RethinkDBEx(JSON(
    ['authentication',m3
    ]))['authentication'];

  m4:='v='+Base64Encode(HMAC_SHA256(HMAC_SHA256(s,'Server Key'),u));

  if t<>m4 then
    raise ERethinkDBAuthenticationFailed.CreateFmt(ErrMsg,[UserName]);
end;

end.
