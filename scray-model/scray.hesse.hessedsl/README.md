## Functions
### Aggregation Functions
### Transformation Functions
Documentation of transformation functions is ordered lexicographically.

#### ABS
Computes the absolute value of the given input. The output type is the same a the input type, axcept that if the input is a String it will be scala.math.BigDecimal
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Int, Long, Float, Double, String, java.math.BigInteger, java.math.BigDecimal, scala.math.BigInt, scala.math.BigDecimal |
| Return value             | Any | absolute value, simil |

#### CASTDOUBLE
trys to cast input parameter to type Double
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Int, Long, Float, Double, String, java.math.BigInteger, java.math.BigDecimal, scala.math.BigInt, scala.math.BigDecimal |
| Return value             | Double | value as an Double |
Similar Functions: CASTLONG, CASTINT, CASTFLOAT, CASTSTRING

#### CASTFLOAT
trys to cast input parameter to type Float
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Int, Long, Float, Double, String, java.math.BigInteger, java.math.BigDecimal, scala.math.BigInt, scala.math.BigDecimal |
| Return value             | Float | value as an Float |
Similar Functions: CASTDOUBLE, CASTINT, CASTLONG, CASTSTRING

#### CASTINT
trys to cast input parameter to type Int
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Int, Long, Float, Double, String, java.math.BigInteger, java.math.BigDecimal, scala.math.BigInt, scala.math.BigDecimal |
| Return value             | Int | value as an Int |
Similar Functions: CASTLONG, CASTDOUBLE, CASTFLOAT, CASTSTRING

#### CASTLONG
trys to cast input parameter to type Long
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Int, Long, Float, Double, String, java.math.BigInteger, java.math.BigDecimal, scala.math.BigInt, scala.math.BigDecimal |
| Return value             | Long | value as an Long |
Similar Functions: CASTDOUBLE, CASTINT, CASTFLOAT, CASTSTRING

#### CASTSTRING
trys to cast input parameter to type String
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Any |
| Return value             | String | value as an String |
Similar Functions: CASTDOUBLE, CASTINT, CASTLONG, CASTFLOAT

#### CRC32
Calculates a checksum using the CRC32 algorithm. If input parameter is a byte array or blob 
the checksum is calculated as is. If it is of any other type, it is treated as a String and
after conversion to an UTF-8-String it is converted to a byte array first.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Any |
| Return value             | Array[Byte] | checksum as a byte array |
Similar Functions: MD5

#### DATE
create a java.util.Date with the property that it is normalized to the day, i.e. the time has been cut off.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | converted date | java.util.Date |
Similar Functions: TIME

#### DAY
returns an Int representing the day-part of the given timestamp. If the given parameter is a java.util.Date
or a Long it will be treaused as a timestamp in milliseconds; Int-values will be treated as timestamps in 
seconds; Strings will be converted using the stringToDate function in the library.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | day of the month | Int |
Similar Functions: MILLISECOND, YEAR, MONTH, MINUTE, SECOND, HOUR

#### HOUR
returns an Int representing the hour-part of the given timestamp. If the given parameter is a java.util.Date
or a Long it will be treaused as a timestamp in milliseconds; Int-values will be treated as timestamps in 
seconds; Strings will be converted using the stringToDate function in the library.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | hour of the day (0-23) | Int |
Similar Functions: DAY, YEAR, MONTH, MINUTE, MILLISECOND, SECOND

#### LENGTH
returns the length of a string. Any type is first converted to a String.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | String |  |
| Return value             | the length of the Input Parameter 1 | Int |

#### MAKEDATE
creates a java.util.Date from 3 input Ints.
| Accepted input parameters | 3 | |
|---------------------------|---|---|
| Input Parameter 1         | year | Int |
| Input Parameter 2         | month | Int |
| Input Parameter 3         | day of month | Int |
| Return value             | created date with time 0h 0m 0s | java.util.Date |
Similar Functions: MAKETIME

#### MAKEDATE
creates a java.util.Date from 3 input Ints.
| Accepted input parameters | 3 | |
|---------------------------|---|---|
| Input Parameter 1         | year | Int |
| Input Parameter 2         | month (1-12) | Int |
| Input Parameter 3         | day of month | Int |
| Input Parameter 4         | hour of the month (0-23) | Int |
| Input Parameter 5         | minute of hour | Int |
| Input Parameter 6         | second | Int |
| Return value             | created date | java.util.Date |
Similar Functions: MAKEDATE

#### MD5
Calculates a checksum using the MD5 algorithm. If input parameter is a byte array or blob 
the checksum is calculated as is. If it is of any other type, it is treated as a String and
after conversion to an UTF-8-String it is converted to a byte array first.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | Any | accepted types: Any |
| Return value             | checksum as a byte array | Array[Byte] |
Similar Functions: CRC32

#### MID
copies a part of a string
| Accepted input parameters | 3 | |
|---------------------------|---|---|
| Input Parameter 1         | complete String | String |
| Input Parameter 2         | start position (0-based) in String | Int-convertable |
| Input Parameter 3         | end position + 1 (0-based) in String | Int-convertable |
| Return value             | copied part | String |

#### MILLESECOND
returns an Int representing the millesecond-part of the given timestamp. If the given parameter is a java.util.Date
or a Long it will be treaused as a timestamp in milliseconds; Int-values will be treated as timestamps in 
seconds; Strings will be converted using the stringToDate function in the library.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | milliseconds of the second | Int |
Similar Functions: DAY, YEAR, MONTH, MINUTE, SECOND, HOUR

#### MINUTE
returns an Int representing the minute-part of the given timestamp. If the given parameter is a java.util.Date
or a Long it will be treaused as a timestamp in milliseconds; Int-values will be treated as timestamps in 
seconds; Strings will be converted using the stringToDate function in the library.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | minutes of the hour | Int |
Similar Functions: DAY, YEAR, MONTH, SECOND, MILLISECOND, HOUR

#### MONTH
returns an Int representing the month-part of the given timestamp. If the given parameter is a java.util.Date
or a Long it will be treaused as a timestamp in milliseconds; Int-values will be treated as timestamps in 
seconds; Strings will be converted using the stringToDate function in the library.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | month of the year (1-12) | Int |
Similar Functions: MILLISECOND, YEAR, DAY, MINUTE, SECOND, HOUR

#### SECOND
returns an Int representing the second-part of the given timestamp. If the given parameter is a java.util.Date
or a Long it will be treaused as a timestamp in milliseconds; Int-values will be treated as timestamps in 
seconds; Strings will be converted using the stringToDate function in the library.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | seconds of the minute | Int |
Similar Functions: DAY, YEAR, MONTH, MINUTE, MILLISECOND, HOUR

#### TIME
create a java.util.Date from various input types
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | converted date | java.util.Date |
Similar Functions: DATE

#### YEAR
returns an Int representing the year-part of the given timestamp. If the given parameter is a java.util.Date
or a Long it will be treaused as a timestamp in milliseconds; Int-values will be treated as timestamps in 
seconds; Strings will be converted using the stringToDate function in the library.
| Accepted input parameters | 1 | |
|---------------------------|---|---|
| Input Parameter 1         | input date | Long [ms], Int [s], String [yyyy-MM-dd, dd.MM.yyyy, MM/dd/yyyy ] |
| Return value             | year | Int |
Similar Functions: MILLISECOND, MONTH, DAY, MINUTE, SECOND, HOUR
			

