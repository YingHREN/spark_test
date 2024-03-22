stage1:
redis read_table .dat格式
for i in range(chunks_stage1):
    get_object("catalog_sales");
    wanted_columns = ['cs_order_number',
                              'cs_ext_ship_cost',
                              'cs_net_profit',
                              'cs_ship_date_sk',
                              'cs_ship_addr_sk',
                              'cs_call_center_sk',
                              'cs_warehouse_sk']
    cs_s = cs[wanted_columns]
    记录cs_order_number
    cs_merged.append(cs_s)
    send_object(cs_s)

2. get_object("customer_address")
   记录cs_order_number
3. get_object("data_dim")
   
4. get_object("call_center")
5. get_object("catalog_returns")
6. 