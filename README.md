# DFMsSqlUnit

A comprehensive Delphi unit for working with Microsoft SQL Server databases using ADO components.

## Overview

DFMsSqlUnit provides a simple and powerful interface for connecting to and working with Microsoft SQL Server databases from Delphi applications. It handles common database operations like querying, transactions, and CRUD operations through an easy-to-use singleton interface.

## Features

- Simple connection management with ADO
- Support for Windows authentication and SQL Server authentication
- Transaction management
- Comprehensive error handling
- CRUD operations (Insert, Update, Delete)
- Stored procedure execution with parameter support
- Utility functions for table management and SQL formatting
- Dataset operations
- Singleton pattern implementation for easy global access

## Installation

1. Add the `DFMsSqlUnit.pas` file to your Delphi project
2. Add the unit to your uses clause
3. Make sure you have the ADO components installed in your Delphi IDE

## Requirements

- Delphi 10.x or newer recommended (should work with older versions too)
- Microsoft SQL Server database
- ADO components

## Usage Examples

### Connecting to a Database

```pascal
// Using Windows Authentication
DFMsSql.BuildConnectionString('ServerName', 'DatabaseName', '', '', True);
if DFMsSql.Connect then
  ShowMessage('Connected successfully!')
else
  ShowMessage('Connection failed: ' + DFMsSql.LastError);

// Using SQL Server Authentication
DFMsSql.BuildConnectionString('ServerName', 'DatabaseName', 'Username', 'Password');
if DFMsSql.Connect then
  ShowMessage('Connected successfully!')
else
  ShowMessage('Connection failed: ' + DFMsSql.LastError);
```

### Executing a Query

```pascal
if DFMsSql.ExecuteQuery('UPDATE Customers SET Active = 1 WHERE CustomerID = 100') then
  ShowMessage('Update successful!')
else
  ShowMessage('Error: ' + DFMsSql.LastError);
```

### Insert Operation

```pascal
var
  CustomerID: Integer;
begin
  CustomerID := DFMsSql.Insert('Customers', 
                             ['Name', 'Email', 'Phone'], 
                             ['John Doe', 'john@example.com', '555-1234']);
  
  if CustomerID > 0 then
    ShowMessage('Customer added with ID: ' + IntToStr(CustomerID))
  else
    ShowMessage('Error adding customer: ' + DFMsSql.LastError);
end;
```

### Update Operation

```pascal
var
  RowsAffected: Integer;
begin
  RowsAffected := DFMsSql.Update('Customers',
                               ['Name', 'Email'],
                               ['John Smith', 'john.smith@example.com'],
                               'CustomerID = 100');
  
  ShowMessage('Updated ' + IntToStr(RowsAffected) + ' records');
end;
```

### Delete Operation

```pascal
var
  RowsAffected: Integer;
begin
  RowsAffected := DFMsSql.Delete('Customers', 'CustomerID = 100');
  
  ShowMessage('Deleted ' + IntToStr(RowsAffected) + ' records');
end;
```

### Working with Datasets

```pascal
var
  Dataset: TDataset;
begin
  Dataset := DFMsSql.OpenDataset('SELECT * FROM Customers ORDER BY Name');
  
  if Assigned(Dataset) then
  try
    while not Dataset.Eof do
    begin
      ShowMessage(Dataset.FieldByName('Name').AsString);
      Dataset.Next;
    end;
  finally
    Dataset.Free;
  end;
end;
```

### Executing Stored Procedures

```pascal
var
  Success: Boolean;
  Result: Integer;
begin
  Success := DFMsSql.ExecuteStoredProc('sp_GetCustomerInfo',
                                     ['@CustomerID', '@IncludeOrders'],
                                     [100, True],
                                     ['@TotalOrders']);
  
  if Success then
  begin
    Result := DFMsSql.GetStoredProcResult('@TotalOrders');
    ShowMessage('Customer has ' + IntToStr(Result) + ' orders');
  end;
end;
```

### Transaction Management

```pascal
begin
  if DFMsSql.BeginTransaction then
  try
    // Do multiple operations
    DFMsSql.ExecuteQuery('UPDATE Products SET Stock = Stock - 1 WHERE ProductID = 10');
    DFMsSql.ExecuteQuery('INSERT INTO OrderItems (OrderID, ProductID) VALUES (1001, 10)');
    
    // Commit if all operations succeed
    DFMsSql.CommitTransaction;
    ShowMessage('Transaction committed');
  except
    on E: Exception do
    begin
      DFMsSql.RollbackTransaction;
      ShowMessage('Transaction rolled back: ' + E.Message);
    end;
  end;
end;
```

## License

This software is released under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
