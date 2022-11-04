--1. List of Persons’ full name, all their fax and phone numbers, 
--as well as the phone number and fax of the company they are working for (if any). 

SELECT FullName, People.FaxNumber, People.PhoneNumber, CustomerName AS 'Company Name', Customers.PhoneNumber, Customers.FaxNumber
FROM Application.People AS People
LEFT JOIN Sales.Customers AS Customers
ON People.PersonID = Customers.AlternateContactPersonID OR People.PersonID = Customers.PrimaryContactPersonID
ORDER BY(SELECT NULL) OFFSET 1 ROW;

-- 2. If the customer's primary contact person has the same phone number as the customer’s phone number, list the customer companies. 

SELECT CustomerName, Customer.PhoneNumber, People.PhoneNumber
FROM Sales.Customers As Customer
LEFT JOIN Application.People AS People
ON Customer.PrimaryContactPersonID = People.PersonID
WHERE Customer.PhoneNumber = People.PhoneNumber;

-- 3.	List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.

SELECT CustomerID
FROM Sales.Orders AS Orders
GROUP BY CustomerID
HAVING MAX(OrderDate) < '20160101';



-- 4.	List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013.

SELECT Description, SUM(OrderLines.OrderedOuters) AS 'Quantity'
FROM Purchasing.PurchaseOrderLines AS OrderLines
LEFT JOIN Purchasing.PurchaseOrders AS Orders
ON Orderlines.PurchaseOrderID = orders.PurchaseOrderID
WHERE DATEPART(yy, Orders.OrderDate) = 2013
GROUP BY DESCRIPTION
ORDER BY QUANTITY DESC;

-- 5.	List of stock items that have at least 10 characters in description.

SELECT StockItemName, LEN(StockItemName)
FROM Warehouse.StockItems
WHERE LEN(StockItemName) >= 10
ORDER BY LEN(StockItemName) ASC;

-- 6.	List of stock items that are not sold to the state of Alabama and Georgia in 2014.

SELECT StockItemName
FROM Warehouse.StockItems
WHERE StockItemName not in (
SELECT DISTINCT Description
FROM Sales.OrderLines AS OrderLine
LEFT JOIN Sales.Orders AS Orders
ON OrderLine.OrderID = Orders.OrderID
LEFT JOIN Sales.Customers AS Customer
ON Orders.CustomerID = Customer.CustomerID
LEFT JOIN Application.Cities as City
ON Customer.DeliveryCityID = City.CityID
LEFT JOIN Application.StateProvinces AS State
ON City.StateProvinceID = State.StateProvinceID
WHERE DATEPART(yy, Orders.OrderDate) = 2014 AND (StateProvinceName = 'Alabama' OR StateProvinceName = 'Georgia')
);

-- 7.	List of States and Avg dates for processing (confirmed delivery date – order date).

SELECT StateProvinceName, AVG(DATEDIFF(HOUR, Orders.OrderDate, Invoice.ConfirmedDeliveryTime)) AS 'Hours'
FROM Sales.Orders AS Orders
LEFT JOIN Sales.Invoices AS Invoice
ON Orders.OrderID = Invoice.OrderID
LEFT JOIN Sales.Customers AS Customer
ON Orders.CustomerID = Customer.CustomerID
LEFT JOIN Application.Cities as City
ON Customer.DeliveryCityID = City.CityID
LEFT JOIN Application.StateProvinces AS State
ON City.StateProvinceID = State.StateProvinceID
GROUP BY StateProvinceName
ORDER BY Hours DESC;

-- 8.	List of States and Avg dates for processing (confirmed delivery date – order date) by month.

SELECT StateProvinceName, AVG(DATEDIFF(HOUR, Orders.OrderDate, Invoice.ConfirmedDeliveryTime)) AS 'Hours', DATEPART(MONTH, OrderDate) AS 'Month'
FROM Sales.Orders AS Orders
LEFT JOIN Sales.Invoices AS Invoice
ON Orders.OrderID = Invoice.OrderID
LEFT JOIN Sales.Customers AS Customer
ON Orders.CustomerID = Customer.CustomerID
LEFT JOIN Application.Cities as City
ON Customer.DeliveryCityID = City.CityID
LEFT JOIN Application.StateProvinces AS State
ON City.StateProvinceID = State.StateProvinceID
GROUP BY StateProvinceName, DATEPART(MONTH, OrderDate)
ORDER BY StateProvinceName, Month;

-- 9.	List of StockItems that the company purchased more than sold in the year of 2015.

SELECT Purchases.Description
FROM(
SELECT Description, SUM(POL.OrderedOuters) AS 'Amount Purchased'
FROM Purchasing.PurchaseOrderLines AS POL
LEFT JOIN Purchasing.PurchaseOrders AS PO
ON POL.PurchaseOrderID = PO.PurchaseOrderID
WHERE DATEPART(yy, PO.OrderDate) = 2015
GROUP BY Description
) AS Purchases
LEFT JOIN
(
SELECT Description, SUM(Quantity) AS 'Amount Sold'
FROM Sales.OrderLines AS OL
INNER JOIN Sales.Orders AS O
ON OL.OrderID = O.OrderID
WHERE DATEPART(yy, O.OrderDate) = 2015
GROUP BY Description
) AS Sales
ON Purchases.Description = Sales.Description
WHERE [Amount Purchased] > [Amount Sold];


-- 10. List of Customers and their phone number, together with the primary contact person’s name, 
-- to whom we did not sell more than 10  mugs (search by name) in the year 2016.

SELECT CustomerName, PhoneNumber, Sum(Count) AS 'Total Mugs Purchased'
FROM
(SELECT Description, CustomerName, PhoneNumber, PrimaryContactPersonID, COUNT(Description) AS 'Count'
FROM Sales.OrderLines AS OL
LEFT JOIN Sales.Orders AS O
ON OL.OrderID = O.OrderID
LEFT JOIN SALES.Customers AS C
ON O.CustomerID = C.CustomerID
WHERE (DESCRIPTION LIKE '%mug%' OR DESCRIPTION LIKE '%Mug%') AND DATEPART(yy, OrderDate) = 2016
GROUP BY Description, CustomerName, PhoneNumber, PrimaryContactPersonID
HAVING COUNT(Description) < 10
) AS Tab
GROUP BY CustomerName, PhoneNumber, PrimaryContactPersonID
HAVING SUM(Count) < 10;

-- 11.	List all the cities that were updated after 2015-01-01.

SELECT CityName, ValidFrom
FROM Application.Cities
WHERE ValidFrom > '2015-01-01';

-- 12.	List all the Order Detail (Stock Item name, delivery address, delivery state, 
-- city, country, customer name, customer contact person name, customer phone, quantity)
-- for the date of 2014-07-01. Info should be relevant to that date.

SELECT Description, DeliveryAddressLine1, DeliveryAddressLine2, StateProvinceName, CityName, CountryName, 
CustomerName, FullName AS 'Contact Person Name', Customer.PhoneNumber, Quantity
FROM Sales.OrderLines as OL
INNER JOIN Sales.Orders as O
ON OL.OrderID = O.OrderID
INNER JOIN Sales.Customers AS Customer
ON O.CustomerID = Customer.CustomerID
LEFT JOIN Application.Cities as City
ON Customer.DeliveryCityID = City.CityID
LEFT JOIN Application.StateProvinces AS State
ON City.StateProvinceID = State.StateProvinceID
LEFT JOIN Application.Countries AS Country
ON State.CountryID = country.CountryID
LEFT JOIN Application.People AS People
ON Customer.PrimaryContactPersonID = People.PersonID
WHERE OrderDate = '2014-07-01';

-- 13.	List of stock item groups and total quantity purchased, total quantity sold, 
-- and the remaining stock quantity (quantity purchased – quantity sold)

SELECT A.StockGroupID, [Quantity Purchased], [Quantity Sold], [Quantity Purchased] - [Quantity Sold] AS 'Remaining'
FROM (
SELECT StockGroupID, SUM(POL.OrderedOuters) AS 'Quantity Purchased'
FROM Purchasing.PurchaseOrderLines AS POL
LEFT JOIN Warehouse.StockItemStockGroups AS Groups
ON POL.StockItemID = Groups.StockItemID
GROUP BY StockGroupID
) AS A
LEFT JOIN 
(
SELECT StockGroupID, SUM(OL.Quantity) AS 'Quantity Sold'
FROM Sales.OrderLines AS OL
INNER JOIN Warehouse.StockItemStockGroups AS Groups
ON OL.StockItemID = GROUPS.StockItemID
GROUP BY StockGroupID
) AS B
ON A.StockGroupID = B.StockGroupID;


-- 14.	List of Cities in the US and the stock item that the city got the most deliveries in 2016. 
-- If the city did not purchase any stock items in 2016, print “No Sales”.

SELECT TAB1.*
FROM (
SELECT Description, CityName, SUM(Quantity) AS 'Quantity Sold'
FROM Sales.OrderLines AS OL
LEFT JOIN SALES.Orders AS O
ON OL.OrderID = O.OrderID
LEFT JOIN SALES.Customers AS C
ON O.CustomerID = C.CustomerID
LEFT JOIN Application.Cities as City
ON C.DeliveryCityID = City.CityID
WHERE DATEPART(yy, OrderDate) = 2016
GROUP BY Description, Cityname
) AS TAB1
LEFT JOIN (
SELECT Description, CityName, SUM(Quantity) AS 'Quantity Sold'
FROM Sales.OrderLines AS OL
LEFT JOIN SALES.Orders AS O
ON OL.OrderID = O.OrderID
LEFT JOIN SALES.Customers AS C
ON O.CustomerID = C.CustomerID
LEFT JOIN Application.Cities as City
ON C.DeliveryCityID = City.CityID
WHERE DATEPART(yy, OrderDate) = 2016
GROUP BY Description, Cityname
) AS TAB2
ON TAB1.CityName = TAB2.CityName AND TAB1.[Quantity Sold] < TAB2.[Quantity Sold]
WHERE TAB2.[Quantity Sold] IS NULL
ORDER BY CityName;

-- 15.	List any orders that had more than one delivery attempt (located in invoice table).

SELECT InvoiceID
FROM SALES.Invoices
WHERE JSON_VALUE(ReturnedDeliveryData, '$.Events[1].Comment') = 'Receiver not present';


-- 16.	List all stock items that are manufactured in China. (Country of Manufacture)

SELECT StockItemName, JSON_VALUE(CustomFields, '$.CountryOfManufacture') As 'Country of Manufacture'
FROM Warehouse.StockItems
WHERE JSON_VALUE(CustomFields, '$.CountryOfManufacture') = 'China';

-- 17.	Total quantity of stock items sold in 2015, group by country of manufacturing.

SELECT JSON_VALUE(CustomFields, '$.CountryOfManufacture') As Country, SUM(Quantity) As Quantity
FROM Sales.OrderLines AS OL
INNER JOIN Sales.Orders AS O
ON OL.OrderID = O.OrderID
INNER JOIN Warehouse.StockItems AS Stock
ON OL.StockItemID = Stock.StockItemID
WHERE DATEPART(yy, OrderDate) = 2015
GROUP BY JSON_VALUE(CustomFields, '$.CountryOfManufacture');

-- 18. Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. 
-- [Stock Group Name, 2013, 2014, 2015, 2016, 2017]

GO
CREATE VIEW Warehouse.Stockview AS
SELECT GroupID, [2013], [2014], [2015], [2016], [2017]
FROM (
SELECT  G.StockGroupID AS GroupID, DATEPART(yy, OrderDate) AS Year, SUM(Quantity) AS 'QuantitySold'
FROM Sales.OrderLines AS OL
INNER JOIN Sales.Orders AS O
ON OL.OrderID = O.OrderID
INNER JOIN Warehouse.StockItemStockGroups AS G
ON OL.StockItemID = G.StockItemID
GROUP BY G.StockGroupID, DATEPART(yy, OrderDate)
) AS SourceTable
PIVOT
(
MAX(QuantitySold)
FOR 
Year in ([2013], [2014], [2015], [2016], [2017])
) AS PivotTable
GO

-- 19.	Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. 
-- [Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, … , Stock Group Name10
GO
CREATE VIEW Warehouse.YearQuantityStockGroup AS
SELECT Year, [1], [2], [3], [4],[5],[6],[7],[8],[9],[10]
FROM (
SELECT  G.StockGroupID AS GroupID, DATEPART(yy, OrderDate) AS Year, SUM(Quantity) AS 'QuantitySold'
FROM Sales.OrderLines AS OL
INNER JOIN Sales.Orders AS O
ON OL.OrderID = O.OrderID
INNER JOIN Warehouse.StockItemStockGroups AS G
ON OL.StockItemID = G.StockItemID
GROUP BY G.StockGroupID, DATEPART(yy, OrderDate)
) AS SourceTable
PIVOT
(
MAX(QuantitySold)
FOR 
GroupID in ([1], [2], [3], [4],[5],[6],[7],[8],[9],[10])
) AS PivotTable
GO

-- 20.	Create a function, input: order id; return: total of that order. 
-- List invoices and use that function to attach the order total to the other fields of invoices. 

GO

CREATE FUNCTION my_function(@order_id int)
RETURNS int AS
BEGIN
RETURN
(SELECT SUM(Quantity)
FROM Sales.InvoiceLines AS IL
JOIN Sales.Invoices AS I
ON IL.InvoiceID = I.InvoiceID AND I.OrderID = @order_id
)
END;

GO

SELECT InvoiceID, OrderID, dbo.my_function(OrderID) AS OrderTotal
FROM Sales.Invoices;

-- 21. Create a new table called ods.Orders. Create a stored procedure, with proper error handling
-- and transactions, that input is a date; when executed, it would find orders of that day, calculate order total, 
-- and save the information (order id, order date, order total, customer id) into the new table. If a given date is
-- already existing in the new table, throw an error and roll back. Execute the stored procedure 5 times using different dates. 


CREATE TABLE ods.Orders(
	OrderID INT PRIMARY KEY,
	OrderDate DATE,
	OrderTotal INT,
	CustomerID INT
);
GO
-- Procedure

CREATE PROCEDURE ods.inputvalues
@DAY DATE
AS 
BEGIN TRY
BEGIN TRANSACTION
IF @DAY IN (SELECT OrderDate FROM ods.Orders)
RAISERROR ('Order Date Already Exists in Table', 15, 1)
ELSE
BEGIN
INSERT INTO ods.Orders(OrderID, OrderDate, OrderTotal, CustomerID)
SELECT OrderID, @DAY AS OrderDate, OrderTotal, CustomerID
FROM
(SELECT O.OrderID, O.CustomerID, SUM(OL.PickedQuantity) AS OrderTotal
FROM Sales.OrderLines AS OL
LEFT JOIN Sales.Orders AS O
ON O.OrderID = OL.OrderID
WHERE O.OrderDate = @DAY
GROUP BY O.OrderID, O.CustomerID) AS TAB
COMMIT TRANSACTION 
END
END TRY
BEGIN CATCH
IF @@TRANCOUNT>0
DECLARE @ErrorMessage NVARCHAR(4000);  
DECLARE @ErrorSeverity INT;  
DECLARE @ErrorState INT;  
SELECT   
@ErrorMessage = ERROR_MESSAGE(),  
@ErrorSeverity = ERROR_SEVERITY(),  
@ErrorState = ERROR_STATE();  
RAISERROR (
@ErrorMessage, -- Message text.  
@ErrorSeverity, -- Severity.  
@ErrorState -- State.  
);  
ROLLBACK TRANSACTION 
END CATCH;

EXEC ods.inputvalues @DAY = '2015-05-01';
EXEC ods.inputvalues @DAY = '2015-01-05';
EXEC ods.inputvalues @DAY = '2015-03-07';
EXEC ods.inputvalues @DAY = '2015-01-03';
EXEC ods.inputvalues @DAY = '2015-04-04';

SELECT *
FROM ODS.Orders;

-- 22. Create a new table called ods.StockItem. It has following columns: [StockItemID], [StockItemName] ,
-- [SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,[LeadTimeDays] ,[QuantityPerOuter] ,
-- [IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,[TypicalWeightPerUnit] ,[MarketingComments]
-- ,[InternalComments], [CountryOfManufacture], [Range], [Shelflife]. Migrate all the data in the original stock item table.

SELECT[StockItemID], [StockItemName], [SupplierID], [ColorID], [UnitPackageID], [OuterPackageID], [Brand], [Size], [LeadTimeDays], [QuantityPerOuter], 
	  [IsChillerStock], [Barcode], [TaxRate], [UnitPrice], [RecommendedRetailPrice], [TypicalWeightPerUnit], [MarketingComments],[InternalComments],
	  JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS CountryOfManufacture, JSON_VALUE(CustomFields,'$.Range') AS Range, 
	  JSON_VALUE(CustomFields,'$.ShelfLife') AS ShelfLife
INTO ods.StockItem
FROM Warehouse.StockItems;

SELECT *
FROM ods.StockItem;

-- 23.	Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the order data prior to the input 
-- date and load the order data that was placed in the next 7 days following the input date.

-- 24.	Consider the JSON file:

DECLARE @json NVARCHAR(MAX)
SET @json=
N'{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":"6",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-025",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}';

SELECT *
FROM OPENJSON(@json)
WITH (StockItemName NVARCHAR(50) '$.StockItemName',
Supplier INT '$.Supplier',
UnitPackageId INT '$.UnitPackageId',
Brand NVARCHAR(50) '$.Brand',
LeadTimeDays INT '$.LeadTimeDays',
QuantityPerOuter INT '$.QuantityPerOuter',
TaxRate  DECIMAL(18,3) '$.TaxRate',
UnitPrice DECIMAL(18,2) '$.UnitPrice',
RecommendedRetailPrice DECIMAL(18,2) '$.RecommendedRetailPrice',
TypicalWeightPerUnit  DECIMAL(18,3) '$.TypicalWeightPerUnit',
[CustomFields.CountryOfManufacture] NVARCHAR(50)  '$.CountryOfManufacture',
[CustomFields.Range] NVARCHAR(50) '$.Range',
OrderDate NVARCHAR(50) '$.OrderDate',
DeliveryMethod NVARCHAR(50) '$.DeliveryMethod',
ExpectedDeliveryDate NVARCHAR(50) '$.ExpectedDeliveryDate',
SupplierReference NVARCHAR(MAX) '$.SupplierReference'
)



-- 25 Revisit your answer in (19). Convert the result in JSON string and save it to the server using TSQL FOR JSON PATH.
SELECT *
FROM Warehouse.YearQuantityStockGroup
FOR JSON AUTO;


-- 26 Revisit your answer in (19). Convert the result into an XML string and save it to the server using TSQL FOR XML PATH.

SELECT *
FROM Warehouse.YearQuantityStockGroup
FOR XML AUTO;

