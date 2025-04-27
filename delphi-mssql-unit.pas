unit DFMsSqlUnit;

interface

uses
  System.SysUtils, System.Classes, Data.DB, Data.Win.ADODB, Vcl.Dialogs;

type
  // Define parameter type for WHERE clauses
  TParamDirection = (pdInput, pdOutput, pdInputOutput, pdReturnValue);
  
  TWhereParam = record
    Name: string;
    Value: Variant;
    DataType: TFieldType;
    Direction: TParamDirection;
  end;
  
  TDFMsSqlConnection = class(TObject)
  private
    FConnectionString: string;
    FConnection: TADOConnection;
    FQuery: TADOQuery;
    FLastError: string;
    FLastErrorCode: Integer;
    FConnected: Boolean;
    FTimeout: Integer;

    procedure SetConnectionString(const Value: string);
    procedure SetTimeout(const Value: Integer);
    function GetConnected: Boolean;
    procedure AssignQueryParams(const Params: array of TWhereParam);
  public
    constructor Create;
    destructor Destroy; override;

    // Connection methods
    function BuildConnectionString(const Server, Database, Username, Password: string;
                                  WindowsAuth: Boolean = False): string;
    function Connect: Boolean;
    function Disconnect: Boolean;
    function TestConnection: Boolean;
    
    // Transaction methods
    function BeginTransaction: Boolean;
    function CommitTransaction: Boolean;
    function RollbackTransaction: Boolean;
    
    // Query methods
    function ExecuteQuery(const SQL: string): Boolean; overload;
    function ExecuteQuery(const SQL: string; const Params: array of TWhereParam): Boolean; overload;
    function ExecuteScalar(const SQL: string): Variant; overload;
    function ExecuteScalar(const SQL: string; const Params: array of TWhereParam): Variant; overload;
    function OpenDataset(const SQL: string): TDataset; overload;
    function OpenDataset(const SQL: string; const Params: array of TWhereParam): TDataset; overload;
    
    // CRUD operations
    function Insert(const TableName: string; const Fields: array of string; 
                   const Values: array of Variant): Integer;
    function Update(const TableName: string; const Fields: array of string;
                   const Values: array of Variant; const WhereClause: string): Integer; overload;
    function Update(const TableName: string; const Fields: array of string;
                   const Values: array of Variant; const WhereClause: string; 
                   const Params: array of TWhereParam): Integer; overload;
    function Delete(const TableName: string; const WhereClause: string): Integer; overload;
    function Delete(const TableName: string; const WhereClause: string; 
                   const Params: array of TWhereParam): Integer; overload;
    
    // Parameterized query builders
    function Select(const TableName: string; const Fields: array of string; 
                   const WhereClause: string = ''): TDataset; overload;
    function Select(const TableName: string; const Fields: array of string; 
                   const WhereClause: string; const Params: array of TWhereParam): TDataset; overload;
    
    // Helper function to create parameter records
    function Param(const Name: string; const Value: Variant; 
                  DataType: TFieldType = ftUnknown; 
                  Direction: TParamDirection = pdInput): TWhereParam;
    
    // Stored procedure methods
    function ExecuteStoredProc(const ProcName: string; 
                              const ParamNames: array of string;
                              const ParamValues: array of Variant;
                              const OutputParams: array of string = nil): Boolean;
    function GetStoredProcResult(const ParamName: string): Variant;
    
    // Utility methods
    function EscapeString(const Value: string): string;
    function GetLastInsertID: Integer;
    function QuotedStr(const Value: string): string;
    function CreateTable(const TableName: string; const ColumnDefs: array of string): Boolean;
    function DropTable(const TableName: string): Boolean;
    function TableExists(const TableName: string): Boolean;
    function GetTableColumns(const TableName: string): TStringList;
    
    // Properties
    property ConnectionString: string read FConnectionString write SetConnectionString;
    property Connection: TADOConnection read FConnection;
    property Query: TADOQuery read FQuery;
    property LastError: string read FLastError;
    property LastErrorCode: Integer read FLastErrorCode;
    property Connected: Boolean read GetConnected;
    property Timeout: Integer read FTimeout write SetTimeout default 30;
  end;

// Simple interface for singleton pattern
function DFMsSql: TDFMsSqlConnection;

implementation

var
  FMsSqlInstance: TDFMsSqlConnection = nil;

function DFMsSql: TDFMsSqlConnection;
begin
  if FMsSqlInstance = nil then
    FMsSqlInstance := TDFMsSqlConnection.Create;
  Result := FMsSqlInstance;
end;

{ TDFMsSqlConnection }

constructor TDFMsSqlConnection.Create;
begin
  inherited Create;
  FConnection := TADOConnection.Create(nil);
  FQuery := TADOQuery.Create(nil);
  FQuery.Connection := FConnection;
  FTimeout := 30;
  FConnection.LoginPrompt := False;
  FConnection.CommandTimeout := FTimeout;
  FConnected := False;
end;

destructor TDFMsSqlConnection.Destroy;
begin
  Disconnect;
  FQuery.Free;
  FConnection.Free;
  inherited;
end;

procedure TDFMsSqlConnection.SetConnectionString(const Value: string);
begin
  if FConnectionString <> Value then
  begin
    FConnectionString := Value;
    FConnection.ConnectionString := Value;
  end;
end;

procedure TDFMsSqlConnection.SetTimeout(const Value: Integer);
begin
  if FTimeout <> Value then
  begin
    FTimeout := Value;
    FConnection.CommandTimeout := Value;
  end;
end;

function TDFMsSqlConnection.GetConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

procedure TDFMsSqlConnection.AssignQueryParams(const Params: array of TWhereParam);
var
  i: Integer;
  ParamItem: TWhereParam;
  ADOParam: TParameter;
begin
  FQuery.Parameters.Clear;
  
  for i := 0 to Length(Params) - 1 do
  begin
    ParamItem := Params[i];
    ADOParam := FQuery.Parameters.CreateParameter(
      ParamItem.Name,
      ParamItem.DataType,
      TParameterDirection(Integer(ParamItem.Direction)),
      0,
      ParamItem.Value
    );
    FQuery.Parameters.Add(ADOParam);
  end;
end;

function TDFMsSqlConnection.Param(const Name: string; const Value: Variant;
                                DataType: TFieldType = ftUnknown;
                                Direction: TParamDirection = pdInput): TWhereParam;
begin
  Result.Name := Name;
  Result.Value := Value;
  Result.DataType := DataType;
  Result.Direction := Direction;
  
  // Auto-detect data type if not specified
  if DataType = ftUnknown then
  begin
    if VarIsStr(Value) then
      Result.DataType := ftString
    else if VarType(Value) = varBoolean then
      Result.DataType := ftBoolean
    else if VarType(Value) = varDate then
      Result.DataType := ftDateTime
    else if VarIsNumeric(Value) then
    begin
      if Frac(Value) = 0 then
        Result.DataType := ftInteger
      else
        Result.DataType := ftFloat;
    end
    else if VarIsNull(Value) then
      Result.DataType := ftString;
  end;
end;

function TDFMsSqlConnection.BuildConnectionString(const Server, Database, Username, 
                                                Password: string;
                                                WindowsAuth: Boolean = False): string;
begin
  Result := 'Provider=SQLOLEDB.1;';
  Result := Result + 'Data Source=' + Server + ';';
  Result := Result + 'Initial Catalog=' + Database + ';';
  
  if WindowsAuth then
    Result := Result + 'Integrated Security=SSPI;'
  else
  begin
    Result := Result + 'Persist Security Info=True;';
    Result := Result + 'User ID=' + Username + ';';
    Result := Result + 'Password=' + Password + ';';
  end;
  
  ConnectionString := Result;
end;

function TDFMsSqlConnection.Connect: Boolean;
begin
  Result := False;
  try
    if FConnection.Connected then
      FConnection.Close;
    
    FConnection.ConnectionString := FConnectionString;
    FConnection.Open;
    Result := FConnection.Connected;
    FConnected := Result;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Disconnect: Boolean;
begin
  Result := False;
  try
    if FConnection.Connected then
      FConnection.Close;
    Result := not FConnection.Connected;
    FConnected := not Result;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.TestConnection: Boolean;
begin
  Result := Connect;
  if Result then
    Disconnect;
end;

function TDFMsSqlConnection.BeginTransaction: Boolean;
begin
  Result := False;
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    FConnection.BeginTrans;
    Result := True;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.CommitTransaction: Boolean;
begin
  Result := False;
  try
    FConnection.CommitTrans;
    Result := True;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.RollbackTransaction: Boolean;
begin
  Result := False;
  try
    FConnection.RollbackTrans;
    Result := True;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.ExecuteQuery(const SQL: string): Boolean;
begin
  Result := False;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    FQuery.Close;
    FQuery.SQL.Text := SQL;
    FQuery.ExecSQL;
    Result := True;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.ExecuteQuery(const SQL: string; 
                                        const Params: array of TWhereParam): Boolean;
begin
  Result := False;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    FQuery.Close;
    FQuery.SQL.Text := SQL;
    AssignQueryParams(Params);
    FQuery.ExecSQL;
    Result := True;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.ExecuteScalar(const SQL: string): Variant;
begin
  Result := Null;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    FQuery.Close;
    FQuery.SQL.Text := SQL;
    FQuery.Open;
    
    if not FQuery.IsEmpty then
      Result := FQuery.Fields[0].Value;
      
    FQuery.Close;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.ExecuteScalar(const SQL: string; 
                                        const Params: array of TWhereParam): Variant;
begin
  Result := Null;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    FQuery.Close;
    FQuery.SQL.Text := SQL;
    AssignQueryParams(Params);
    FQuery.Open;
    
    if not FQuery.IsEmpty then
      Result := FQuery.Fields[0].Value;
      
    FQuery.Close;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.OpenDataset(const SQL: string): TDataset;
var
  Query: TADOQuery;
begin
  Result := nil;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    Query := TADOQuery.Create(nil);
    Query.Connection := FConnection;
    Query.SQL.Text := SQL;
    Query.Open;
    
    Result := Query;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.OpenDataset(const SQL: string; 
                                      const Params: array of TWhereParam): TDataset;
var
  Query: TADOQuery;
  i: Integer;
  ParamItem: TWhereParam;
  ADOParam: TParameter;
begin
  Result := nil;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    Query := TADOQuery.Create(nil);
    Query.Connection := FConnection;
    Query.SQL.Text := SQL;
    
    // Assign parameters to the new query
    for i := 0 to Length(Params) - 1 do
    begin
      ParamItem := Params[i];
      ADOParam := Query.Parameters.CreateParameter(
        ParamItem.Name,
        ParamItem.DataType,
        TParameterDirection(Integer(ParamItem.Direction)),
        0,
        ParamItem.Value
      );
      Query.Parameters.Add(ADOParam);
    end;
    
    Query.Open;
    Result := Query;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Insert(const TableName: string; const Fields: array of string;
                                 const Values: array of Variant): Integer;
var
  SQL: string;
  i: Integer;
  FieldList, ValueList: string;
begin
  Result := -1;
  FLastError := '';
  FLastErrorCode := 0;
  
  if Length(Fields) <> Length(Values) then
  begin
    FLastError := 'Fields and Values arrays must have the same length';
    Exit;
  end;
  
  try
    // Prepare field list and value list
    FieldList := '';
    ValueList := '';
    
    for i := 0 to Length(Fields) - 1 do
    begin
      if i > 0 then
      begin
        FieldList := FieldList + ', ';
        ValueList := ValueList + ', ';
      end;
      
      FieldList := FieldList + Fields[i];
      
      if VarIsNull(Values[i]) then
        ValueList := ValueList + 'NULL'
      else if VarIsStr(Values[i]) then
        ValueList := ValueList + QuotedStr(Values[i])
      else if VarType(Values[i]) = varBoolean then
      begin
        if Values[i] then
          ValueList := ValueList + '1'
        else
          ValueList := ValueList + '0';
      end
      else if VarType(Values[i]) = varDate then
        ValueList := ValueList + QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Values[i]))
      else
        ValueList := ValueList + VarToStr(Values[i]);
    end;
    
    // Build SQL
    SQL := Format('INSERT INTO %s (%s) VALUES (%s)', [TableName, FieldList, ValueList]);
    
    if ExecuteQuery(SQL) then
      Result := GetLastInsertID;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Update(const TableName: string; const Fields: array of string;
                                 const Values: array of Variant; const WhereClause: string): Integer;
var
  SQL: string;
  i: Integer;
  SetClause: string;
begin
  Result := -1;
  FLastError := '';
  FLastErrorCode := 0;
  
  if Length(Fields) <> Length(Values) then
  begin
    FLastError := 'Fields and Values arrays must have the same length';
    Exit;
  end;
  
  try
    // Prepare SET clause
    SetClause := '';
    
    for i := 0 to Length(Fields) - 1 do
    begin
      if i > 0 then
        SetClause := SetClause + ', ';
      
      SetClause := SetClause + Fields[i] + ' = ';
      
      if VarIsNull(Values[i]) then
        SetClause := SetClause + 'NULL'
      else if VarIsStr(Values[i]) then
        SetClause := SetClause + QuotedStr(Values[i])
      else if VarType(Values[i]) = varBoolean then
      begin
        if Values[i] then
          SetClause := SetClause + '1'
        else
          SetClause := SetClause + '0';
      end
      else if VarType(Values[i]) = varDate then
        SetClause := SetClause + QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Values[i]))
      else
        SetClause := SetClause + VarToStr(Values[i]);
    end;
    
    // Build SQL
    SQL := Format('UPDATE %s SET %s', [TableName, SetClause]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
    
    if ExecuteQuery(SQL) then
      Result := FQuery.RowsAffected;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Update(const TableName: string; const Fields: array of string;
                                 const Values: array of Variant; const WhereClause: string;
                                 const Params: array of TWhereParam): Integer;
var
  SQL: string;
  i: Integer;
  SetClause: string;
begin
  Result := -1;
  FLastError := '';
  FLastErrorCode := 0;
  
  if Length(Fields) <> Length(Values) then
  begin
    FLastError := 'Fields and Values arrays must have the same length';
    Exit;
  end;
  
  try
    // Prepare SET clause
    SetClause := '';
    
    for i := 0 to Length(Fields) - 1 do
    begin
      if i > 0 then
        SetClause := SetClause + ', ';
      
      SetClause := SetClause + Fields[i] + ' = ';
      
      if VarIsNull(Values[i]) then
        SetClause := SetClause + 'NULL'
      else if VarIsStr(Values[i]) then
        SetClause := SetClause + QuotedStr(Values[i])
      else if VarType(Values[i]) = varBoolean then
      begin
        if Values[i] then
          SetClause := SetClause + '1'
        else
          SetClause := SetClause + '0';
      end
      else if VarType(Values[i]) = varDate then
        SetClause := SetClause + QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Values[i]))
      else
        SetClause := SetClause + VarToStr(Values[i]);
    end;
    
    // Build SQL
    SQL := Format('UPDATE %s SET %s', [TableName, SetClause]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
    
    if ExecuteQuery(SQL, Params) then
      Result := FQuery.RowsAffected;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Delete(const TableName: string; const WhereClause: string): Integer;
var
  SQL: string;
begin
  Result := -1;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    // Build SQL
    SQL := Format('DELETE FROM %s', [TableName]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
    
    if ExecuteQuery(SQL) then
      Result := FQuery.RowsAffected;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Delete(const TableName: string; const WhereClause: string;
                                 const Params: array of TWhereParam): Integer;
var
  SQL: string;
begin
  Result := -1;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    // Build SQL
    SQL := Format('DELETE FROM %s', [TableName]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
    
    if ExecuteQuery(SQL, Params) then
      Result := FQuery.RowsAffected;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Select(const TableName: string; const Fields: array of string;
                                 const WhereClause: string = ''): TDataset;
var
  SQL: string;
  i: Integer;
  FieldList: string;
begin
  Result := nil;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    // Prepare field list
    FieldList := '';
    
    if Length(Fields) = 0 then
      FieldList := '*'
    else
    begin
      for i := 0 to Length(Fields) - 1 do
      begin
        if i > 0 then
          FieldList := FieldList + ', ';
        
        FieldList := FieldList + Fields[i];
      end;
    end;
    
    // Build SQL
    SQL := Format('SELECT %s FROM %s', [FieldList, TableName]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
    
    Result := OpenDataset(SQL);
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.Select(const TableName: string; const Fields: array of string;
                                 const WhereClause: string; 
                                 const Params: array of TWhereParam): TDataset;
var
  SQL: string;
  i: Integer;
  FieldList: string;
begin
  Result := nil;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    // Prepare field list
    FieldList := '';
    
    if Length(Fields) = 0 then
      FieldList := '*'
    else
    begin
      for i := 0 to Length(Fields) - 1 do
      begin
        if i > 0 then
          FieldList := FieldList + ', ';
        
        FieldList := FieldList + Fields[i];
      end;
    end;
    
    // Build SQL
    SQL := Format('SELECT %s FROM %s', [FieldList, TableName]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
    
    Result := OpenDataset(SQL, Params);
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.ExecuteStoredProc(const ProcName: string;
                                            const ParamNames: array of string;
                                            const ParamValues: array of Variant;
                                            const OutputParams: array of string = nil): Boolean;
var
  Proc: TADOStoredProc;
  i, j: Integer;
  IsOutput: Boolean;
begin
  Result := False;
  FLastError := '';
  FLastErrorCode := 0;
  
  if Length(ParamNames) <> Length(ParamValues) then
  begin
    FLastError := 'ParamNames and ParamValues arrays must have the same length';
    Exit;
  end;
  
  try
    if not FConnection.Connected then
      if not Connect then
        Exit;
    
    Proc := TADOStoredProc.Create(nil);
    try
      Proc.Connection := FConnection;
      Proc.ProcedureName := ProcName;
      Proc.Parameters.Clear;
      Proc.Prepared := True;
      
      // Set parameters
      for i := 0 to Length(ParamNames) - 1 do
      begin
        // Check if parameter is output
        IsOutput := False;
        if Length(OutputParams) > 0 then
        begin
          for j := 0 to Length(OutputParams) - 1 do
          begin
            if SameText(ParamNames[i], OutputParams[j]) then
            begin
              IsOutput := True;
              Break;
            end;
          end;
        end;
        
        with Proc.Parameters.AddParameter do
        begin
          Name := ParamNames[i];
          Value := ParamValues[i];
          
          if IsOutput then
            Direction := pdOutput;
        end;
      end;
      
      Proc.ExecProc;
      Result := True;
      
      // Copy parameters to query for later retrieval
      FQuery.Close;
      FQuery.SQL.Text := 'SELECT 1';
      FQuery.Parameters.Clear;
      
      for i := 0 to Proc.Parameters.Count - 1 do
      begin
        if Proc.Parameters[i].Direction = pdOutput then
        begin
          with FQuery.Parameters.AddParameter do
          begin
            Name := Proc.Parameters[i].Name;
            Value := Proc.Parameters[i].Value;
          end;
        end;
      end;
    finally
      Proc.Free;
    end;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.GetStoredProcResult(const ParamName: string): Variant;
var
  i: Integer;
begin
  Result := Null;
  
  for i := 0 to FQuery.Parameters.Count - 1 do
  begin
    if SameText(FQuery.Parameters[i].Name, ParamName) then
    begin
      Result := FQuery.Parameters[i].Value;
      Break;
    end;
  end;
end;

function TDFMsSqlConnection.EscapeString(const Value: string): string;
begin
  // Simple SQL injection prevention by doubling single quotes
  Result := StringReplace(Value, '''', '''''', [rfReplaceAll]);
end;

function TDFMsSqlConnection.GetLastInsertID: Integer;
begin
  Result := StrToIntDef(VarToStr(ExecuteScalar('SELECT @@IDENTITY')), -1);
end;

function TDFMsSqlConnection.QuotedStr(const Value: string): string;
begin
  Result := '''' + EscapeString(Value) + '''';
end;

function TDFMsSqlConnection.CreateTable(const TableName: string; 
                                       const ColumnDefs: array of string): Boolean;
var
  SQL: string;
  i: Integer;
  Columns: string;
begin
  Result := False;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    Columns := '';
    
    for i := 0 to Length(ColumnDefs) - 1 do
    begin
      if i > 0 then
        Columns := Columns + ', ';
      
      Columns := Columns + ColumnDefs[i];
    end;
    
    SQL := Format('CREATE TABLE %s (%s)', [TableName, Columns]);
    Result := ExecuteQuery(SQL);
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.DropTable(const TableName: string): Boolean;
var
  SQL: string;
begin
  Result := False;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    SQL := Format('DROP TABLE %s', [TableName]);
    Result := ExecuteQuery(SQL);
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.TableExists(const TableName: string): Boolean;
var
  SQL: string;
  Params: array of TWhereParam;
begin
  Result := False;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    SQL := 'SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES ' +
           'WHERE TABLE_TYPE = ''BASE TABLE'' AND TABLE_NAME = :TableName';
    
    SetLength(Params, 1);
    Params[0] := Param('TableName', TableName, ftString);
    
    var V := ExecuteScalar(SQL, Params);
    
    if not VarIsNull(V) then
      Result := (V > 0);
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

function TDFMsSqlConnection.GetTableColumns(const TableName: string): TStringList;
var
  SQL: string;
  Dataset: TDataset;
  Params: array of TWhereParam;
begin
  Result := TStringList.Create;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    SQL := 'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS ' +
           'WHERE TABLE_NAME = :TableName ORDER BY ORDINAL_POSITION';
    
    SetLength(Params, 1);
    Params[0] := Param('TableName', TableName, ftString);
    
    Dataset := OpenDataset(SQL, Params);
    
    if Assigned(Dataset) then
    try
      while not Dataset.Eof do
      begin
        Result.Add(Format('%s|%s', [Dataset.FieldByName('COLUMN_NAME').AsString,
                                   Dataset.FieldByName('DATA_TYPE').AsString]));
        Dataset.Next;
      end;
    finally
      Dataset.Free;
    end;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
      FreeAndNil(Result);
    end;
  end;
end;

// New method for parameterized INSERT operations
function TDFMsSqlConnection.InsertWithParams(const TableName: string; 
                                           const Fields: array of string;
                                           const Params: array of TWhereParam): Integer;
var
  SQL: string;
  i: Integer;
  FieldList, ParamList: string;
begin
  Result := -1;
  FLastError := '';
  FLastErrorCode := 0;
  
  if Length(Fields) <> Length(Params) then
  begin
    FLastError := 'Fields and Params arrays must have the same length';
    Exit;
  end;
  
  try
    // Prepare field list and parameter list
    FieldList := '';
    ParamList := '';
    
    for i := 0 to Length(Fields) - 1 do
    begin
      if i > 0 then
      begin
        FieldList := FieldList + ', ';
        ParamList := ParamList + ', ';
      end;
      
      FieldList := FieldList + Fields[i];
      ParamList := ParamList + ':' + Params[i].Name;
    end;
    
    // Build SQL
    SQL := Format('INSERT INTO %s (%s) VALUES (%s)', [TableName, FieldList, ParamList]);
    
    if ExecuteQuery(SQL, Params) then
      Result := GetLastInsertID;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

// Helper method to create a parameterized WHERE clause
function TDFMsSqlConnection.BuildWhereClause(const Conditions: array of string; 
                                           const Operator: string = 'AND'): string;
var
  i: Integer;
begin
  Result := '';
  
  for i := 0 to Length(Conditions) - 1 do
  begin
    if i > 0 then
      Result := Result + ' ' + Operator + ' ';
    
    Result := Result + '(' + Conditions[i] + ')';
  end;
end;

// Helper method to execute a simple parameterized query and return number of affected records
function TDFMsSqlConnection.ExecuteWithParams(const SQL: string; 
                                            const Params: array of TWhereParam): Integer;
begin
  Result := -1;
  
  if ExecuteQuery(SQL, Params) then
    Result := FQuery.RowsAffected;
end;

// Helper method for simple parameterized query with multiple WHERE conditions
function TDFMsSqlConnection.SimpleSelect(const TableName: string; 
                                        const Fields: array of string;
                                        const Conditions: array of string;
                                        const Params: array of TWhereParam;
                                        const Operator: string = 'AND'): TDataset;
var
  WhereClause: string;
begin
  WhereClause := BuildWhereClause(Conditions, Operator);
  Result := Select(TableName, Fields, WhereClause, Params);
end;

// Helper method for simple parameterized query that returns a single row as a dictionary
function TDFMsSqlConnection.SelectSingleRow(const TableName: string;
                                          const Fields: array of string;
                                          const WhereClause: string;
                                          const Params: array of TWhereParam): TDictionary<string, Variant>;
var
  Dataset: TDataset;
  i: Integer;
begin
  Result := TDictionary<string, Variant>.Create;
  
  Dataset := Select(TableName, Fields, WhereClause, Params);
  
  if Assigned(Dataset) then
  try
    if not Dataset.IsEmpty then
    begin
      for i := 0 to Dataset.FieldCount - 1 do
        Result.Add(Dataset.Fields[i].FieldName, Dataset.Fields[i].Value);
    end;
  finally
    Dataset.Free;
  end;
end;

// Improved version of Update that uses parameterized field values
function TDFMsSqlConnection.UpdateWithParams(const TableName: string;
                                           const Fields: array of string;
                                           const FieldParams: array of TWhereParam;
                                           const WhereClause: string;
                                           const WhereParams: array of TWhereParam): Integer;
var
  SQL: string;
  i: Integer;
  SetClause: string;
  AllParams: array of TWhereParam;
  FieldCount, WhereCount: Integer;
begin
  Result := -1;
  FLastError := '';
  FLastErrorCode := 0;
  
  if Length(Fields) <> Length(FieldParams) then
  begin
    FLastError := 'Fields and FieldParams arrays must have the same length';
    Exit;
  end;
  
  try
    // Prepare SET clause
    SetClause := '';
    
    for i := 0 to Length(Fields) - 1 do
    begin
      if i > 0 then
        SetClause := SetClause + ', ';
      
      SetClause := SetClause + Fields[i] + ' = :' + FieldParams[i].Name;
    end;
    
    // Build SQL
    SQL := Format('UPDATE %s SET %s', [TableName, SetClause]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
    
    // Combine field and where parameters
    FieldCount := Length(FieldParams);
    WhereCount := Length(WhereParams);
    SetLength(AllParams, FieldCount + WhereCount);
    
    for i := 0 to FieldCount - 1 do
      AllParams[i] := FieldParams[i];
      
    for i := 0 to WhereCount - 1 do
      AllParams[FieldCount + i] := WhereParams[i];
    
    if ExecuteQuery(SQL, AllParams) then
      Result := FQuery.RowsAffected;
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

// Helper method for preparing IN clause parameters
function TDFMsSqlConnection.PrepareInClause(const FieldName: string; 
                                          const Values: array of Variant;
                                          out Params: array of TWhereParam): string;
var
  i: Integer;
  ParamNames: array of string;
begin
  SetLength(ParamNames, Length(Values));
  SetLength(Params, Length(Values));
  
  Result := FieldName + ' IN (';
  
  for i := 0 to Length(Values) - 1 do
  begin
    if i > 0 then
      Result := Result + ', ';
      
    ParamNames[i] := FieldName + 'In' + IntToStr(i);
    Result := Result + ':' + ParamNames[i];
    Params[i] := Param(ParamNames[i], Values[i]);
  end;
  
  Result := Result + ')';
end;

// Example method for pagination using parameterized queries
function TDFMsSqlConnection.SelectWithPaging(const TableName: string;
                                           const Fields: array of string;
                                           const WhereClause: string;
                                           const OrderByClause: string;
                                           const PageNumber, PageSize: Integer;
                                           const WhereParams: array of TWhereParam): TDataset;
var
  SQL: string;
  OffsetValue: Integer;
  PagingParams: array of TWhereParam;
  AllParams: array of TWhereParam;
  i, WhereCount: Integer;
begin
  Result := nil;
  FLastError := '';
  FLastErrorCode := 0;
  
  try
    // Calculate offset
    OffsetValue := (PageNumber - 1) * PageSize;
    
    // Build field list
    var FieldList := '';
    if Length(Fields) = 0 then
      FieldList := '*'
    else
    begin
      for i := 0 to Length(Fields) - 1 do
      begin
        if i > 0 then
          FieldList := FieldList + ', ';
        
        FieldList := FieldList + Fields[i];
      end;
    end;
    
    // Build SQL with pagination
    SQL := Format('SELECT %s FROM %s', [FieldList, TableName]);
    
    if WhereClause <> '' then
      SQL := SQL + ' WHERE ' + WhereClause;
      
    if OrderByClause <> '' then
      SQL := SQL + ' ORDER BY ' + OrderByClause;
      
    SQL := SQL + ' OFFSET :Offset ROWS FETCH NEXT :PageSize ROWS ONLY';
    
    // Create paging parameters
    SetLength(PagingParams, 2);
    PagingParams[0] := Param('Offset', OffsetValue, ftInteger);
    PagingParams[1] := Param('PageSize', PageSize, ftInteger);
    
    // Combine where and paging parameters
    WhereCount := Length(WhereParams);
    SetLength(AllParams, WhereCount + 2);
    
    for i := 0 to WhereCount - 1 do
      AllParams[i] := WhereParams[i];
      
    AllParams[WhereCount] := PagingParams[0];
    AllParams[WhereCount + 1] := PagingParams[1];
    
    // Execute query
    Result := OpenDataset(SQL, AllParams);
  except
    on E: Exception do
    begin
      FLastError := E.Message;
      FLastErrorCode := -1;
    end;
  end;
end;

initialization

finalization
  if FMsSqlInstance <> nil then
    FreeAndNil(FMsSqlInstance);

end.
