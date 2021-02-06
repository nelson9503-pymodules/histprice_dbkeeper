# histprice_dbkeeper

Historical Price Database Keeper do all complicated things so users can simply update and query the data.

## Methods Discovery

**func |** update ( symbol: `str`, data: `dict`, skipUpdate: `bool` )

**func |** query_price ( symbol: `str`, start_time_stamp: `int`, end_time_stamp: `int` ) **->** query_data: `dict` / false_if_symbol_not_exists: `bool`

**func |** query_master_info ( symbol: `str` ) **->** master_info: `dict` / false_if_symbol_not_exists: `bool`

**func |** query_full_master_info ( ) **->** full_master_info: `dict`
