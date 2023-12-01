# 安全的SQL
## 介绍
检查sql是否安全。  
安全：无副作用，仅执行查询。

## 使用方式
### context.Context参数
* `Allow-Func-Ident`：字符串列表
* `Allow-Func-Name`：字符串列表
* `Allow-Col-Name`：字符串列表
* `Allow-DataBase-Name`：字符串列表
* `Allow-Table-Name`：字符串列表

### 调用
调用`CheckSQL`，传入`ctx`和`query`。
返回值：是否安全、失败原因、错误。
