"""database/schema.py — canonical column schemas for Makro & Lotus's"""
from dataclasses import dataclass, field
from typing import List

@dataclass
class MakroSchema:
    # Raw upload columns (match the real Excel)
    DATE:         str = "Date"
    MONTH:        str = "Month"
    LOC:          str = "Loc Number"
    CUST_GROUP:   str = "Customer Main Group Name"
    CUST_NUM:     str = "Customer Number"
    CUSTOMER:     str = "Customer Name"
    REGION:       str = "Region Name"
    DEPT:         str = "Department"
    DIVISION:     str = "Division"
    ITEM:         str = "Item"
    REVENUE:      str = "Net Sales Amt"
    PROFIT:       str = "Net Profit"
    CHANNEL_FLAG: str = "OCS Online/Offline Flag"
    SUB_CHANNEL:  str = "OCS Sub Sales Channel"

    # Derived columns added during cleaning
    WEEK:     str = "Week"
    DOW:      str = "DayOfWeek"       # 0=Mon…6=Sun
    DOW_NAME: str = "DayName"

    # Column aliases: raw name (lower) → canonical
    ALIASES: dict = field(default_factory=lambda: {
        "date":                    "Date",
        "transaction_date":        "Date",
        "trans_date":              "Date",
        "order_date":              "Date",
        "month":                   "Month",
        "loc number":              "Loc Number",
        "loc_number":              "Loc Number",
        "location":                "Loc Number",
        "customer main group name":"Customer Main Group Name",
        "customer_main_group_name":"Customer Main Group Name",
        "cust_group":              "Customer Main Group Name",
        "segment":                 "Customer Main Group Name",
        "customer number":         "Customer Number",
        "customer_number":         "Customer Number",
        "cust_id":                 "Customer Number",
        "customer_id":             "Customer Number",
        "customer name":           "Customer Name",
        "customer_name":           "Customer Name",
        "cust_name":               "Customer Name",
        "customer":                "Customer Name",
        "region name":             "Region Name",
        "region_name":             "Region Name",
        "region":                  "Region Name",
        "department":              "Department",
        "dept":                    "Department",
        "division":                "Division",
        "item":                    "Item",
        "sku":                     "Item",
        "product":                 "Item",
        "net sales amt":           "Net Sales Amt",
        "net_sales_amt":           "Net Sales Amt",
        "revenue":                 "Net Sales Amt",
        "sales":                   "Net Sales Amt",
        "net profit":              "Net Profit",
        "net_profit":              "Net Profit",
        "profit":                  "Net Profit",
        "ocs online/offline flag": "OCS Online/Offline Flag",
        "channel":                 "OCS Online/Offline Flag",
        "ocs sub sales channel":   "OCS Sub Sales Channel",
        "sub_channel":             "OCS Sub Sales Channel",
    })

    REQUIRED_AFTER_CLEAN: List[str] = field(default_factory=lambda: [
        "Date", "Customer Number", "Customer Name",
        "Region Name", "Item", "Net Sales Amt",
    ])

@dataclass
class LotusSchema:
    """Placeholder — future manual-entry pipeline."""
    DATE:       str = "Visit_Date"
    STORE_CODE: str = "Store_Code"
    STORE_NAME: str = "Store_Name"
    ZONE:       str = "Zone"
    SKU_ID:     str = "SKU_ID"
    SKU_NAME:   str = "Product_Name"
    FACING:     str = "Shelf_Facing"
    STOCK_QTY:  str = "Stock_On_Hand"
    ORDER_QTY:  str = "Suggested_Order"
    NOTE:       str = "Salesperson_Note"

MAKRO = MakroSchema()
LOTUS = LotusSchema()
