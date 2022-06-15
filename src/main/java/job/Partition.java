package job;

import lombok.Data;
import lombok.SneakyThrows;
import main.Source;
import util.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@Data
public class Partition implements Run {

  private static String catalog_returns =
      "create table catalog_returns\n"
          + "(\n"
          + "    cr_returned_date_sk       integer                       ,\n"
          + "    cr_returned_time_sk       integer                       ,\n"
          + "    cr_item_sk                integer               not null,\n"
          + "    cr_refunded_customer_sk   integer                       ,\n"
          + "    cr_refunded_cdemo_sk      integer                       ,\n"
          + "    cr_refunded_hdemo_sk      integer                       ,\n"
          + "    cr_refunded_addr_sk       integer                       ,\n"
          + "    cr_returning_customer_sk  integer                       ,\n"
          + "    cr_returning_cdemo_sk     integer                       ,\n"
          + "    cr_returning_hdemo_sk     integer                       ,\n"
          + "    cr_returning_addr_sk      integer                       ,\n"
          + "    cr_call_center_sk         integer                       ,\n"
          + "    cr_catalog_page_sk        integer                       ,\n"
          + "    cr_ship_mode_sk           integer                       ,\n"
          + "    cr_warehouse_sk           integer                       ,\n"
          + "    cr_reason_sk              integer                       ,\n"
          + "    cr_order_number           integer               not null,\n"
          + "    cr_return_quantity        integer                       ,\n"
          + "    cr_return_amount          decimal(7,2)                  ,\n"
          + "    cr_return_tax             decimal(7,2)                  ,\n"
          + "    cr_return_amt_inc_tax     decimal(7,2)                  ,\n"
          + "    cr_fee                    decimal(7,2)                  ,\n"
          + "    cr_return_ship_cost       decimal(7,2)                  ,\n"
          + "    cr_refunded_cash          decimal(7,2)                  ,\n"
          + "    cr_reversed_charge        decimal(7,2)                  ,\n"
          + "    cr_store_credit           decimal(7,2)                  ,\n"
          + "    cr_net_loss               decimal(7,2)                  ,\n"
          + "    primary key (cr_item_sk, cr_order_number, cr_returned_date_sk)\n"
          + ")";

  private static String catalog_sales =
      "create table catalog_sales\n"
          + "(\n"
          + "    cs_sold_date_sk           integer                       ,\n"
          + "    cs_sold_time_sk           integer                       ,\n"
          + "    cs_ship_date_sk           integer                       ,\n"
          + "    cs_bill_customer_sk       integer                       ,\n"
          + "    cs_bill_cdemo_sk          integer                       ,\n"
          + "    cs_bill_hdemo_sk          integer                       ,\n"
          + "    cs_bill_addr_sk           integer                       ,\n"
          + "    cs_ship_customer_sk       integer                       ,\n"
          + "    cs_ship_cdemo_sk          integer                       ,\n"
          + "    cs_ship_hdemo_sk          integer                       ,\n"
          + "    cs_ship_addr_sk           integer                       ,\n"
          + "    cs_call_center_sk         integer                       ,\n"
          + "    cs_catalog_page_sk        integer                       ,\n"
          + "    cs_ship_mode_sk           integer                       ,\n"
          + "    cs_warehouse_sk           integer                       ,\n"
          + "    cs_item_sk                integer               not null,\n"
          + "    cs_promo_sk               integer                       ,\n"
          + "    cs_order_number           integer               not null,\n"
          + "    cs_quantity               integer                       ,\n"
          + "    cs_wholesale_cost         decimal(7,2)                  ,\n"
          + "    cs_list_price             decimal(7,2)                  ,\n"
          + "    cs_sales_price            decimal(7,2)                  ,\n"
          + "    cs_ext_discount_amt       decimal(7,2)                  ,\n"
          + "    cs_ext_sales_price        decimal(7,2)                  ,\n"
          + "    cs_ext_wholesale_cost     decimal(7,2)                  ,\n"
          + "    cs_ext_list_price         decimal(7,2)                  ,\n"
          + "    cs_ext_tax                decimal(7,2)                  ,\n"
          + "    cs_coupon_amt             decimal(7,2)                  ,\n"
          + "    cs_ext_ship_cost          decimal(7,2)                  ,\n"
          + "    cs_net_paid               decimal(7,2)                  ,\n"
          + "    cs_net_paid_inc_tax       decimal(7,2)                  ,\n"
          + "    cs_net_paid_inc_ship      decimal(7,2)                  ,\n"
          + "    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,\n"
          + "    cs_net_profit             decimal(7,2)                  ,\n"
          + "    primary key (cs_item_sk, cs_order_number, cs_sold_date_sk)\n"
          + ")";

  private static String inventory =
      "create table inventory\n"
          + "(\n"
          + "    inv_date_sk               integer               not null,\n"
          + "    inv_item_sk               integer               not null,\n"
          + "    inv_warehouse_sk          integer               not null,\n"
          + "    inv_quantity_on_hand      integer                       ,\n"
          + "    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)\n"
          + ")";

  private static String store_returns =
      "create table store_returns\n"
          + "(\n"
          + "    sr_returned_date_sk       integer                       ,\n"
          + "    sr_return_time_sk         integer                       ,\n"
          + "    sr_item_sk                integer               not null,\n"
          + "    sr_customer_sk            integer                       ,\n"
          + "    sr_cdemo_sk               integer                       ,\n"
          + "    sr_hdemo_sk               integer                       ,\n"
          + "    sr_addr_sk                integer                       ,\n"
          + "    sr_store_sk               integer                       ,\n"
          + "    sr_reason_sk              integer                       ,\n"
          + "    sr_ticket_number          integer               not null,\n"
          + "    sr_return_quantity        integer                       ,\n"
          + "    sr_return_amt             decimal(7,2)                  ,\n"
          + "    sr_return_tax             decimal(7,2)                  ,\n"
          + "    sr_return_amt_inc_tax     decimal(7,2)                  ,\n"
          + "    sr_fee                    decimal(7,2)                  ,\n"
          + "    sr_return_ship_cost       decimal(7,2)                  ,\n"
          + "    sr_refunded_cash          decimal(7,2)                  ,\n"
          + "    sr_reversed_charge        decimal(7,2)                  ,\n"
          + "    sr_store_credit           decimal(7,2)                  ,\n"
          + "    sr_net_loss               decimal(7,2)                  ,\n"
          + "    primary key (sr_item_sk, sr_ticket_number, sr_returned_date_sk)\n"
          + ")";

  private static String store_sales =
      "create table store_sales\n"
          + "(\n"
          + "    ss_sold_date_sk           integer                       ,\n"
          + "    ss_sold_time_sk           integer                       ,\n"
          + "    ss_item_sk                integer               not null,\n"
          + "    ss_customer_sk            integer                       ,\n"
          + "    ss_cdemo_sk               integer                       ,\n"
          + "    ss_hdemo_sk               integer                       ,\n"
          + "    ss_addr_sk                integer                       ,\n"
          + "    ss_store_sk               integer                       ,\n"
          + "    ss_promo_sk               integer                       ,\n"
          + "    ss_ticket_number          integer               not null,\n"
          + "    ss_quantity               integer                       ,\n"
          + "    ss_wholesale_cost         decimal(7,2)                  ,\n"
          + "    ss_list_price             decimal(7,2)                  ,\n"
          + "    ss_sales_price            decimal(7,2)                  ,\n"
          + "    ss_ext_discount_amt       decimal(7,2)                  ,\n"
          + "    ss_ext_sales_price        decimal(7,2)                  ,\n"
          + "    ss_ext_wholesale_cost     decimal(7,2)                  ,\n"
          + "    ss_ext_list_price         decimal(7,2)                  ,\n"
          + "    ss_ext_tax                decimal(7,2)                  ,\n"
          + "    ss_coupon_amt             decimal(7,2)                  ,\n"
          + "    ss_net_paid               decimal(7,2)                  ,\n"
          + "    ss_net_paid_inc_tax       decimal(7,2)                  ,\n"
          + "    ss_net_profit             decimal(7,2)                  ,\n"
          + "    primary key (ss_item_sk, ss_ticket_number, ss_sold_date_sk)\n"
          + ")";

  private static String web_returns =
      "create table web_returns\n"
          + "(\n"
          + "    wr_returned_date_sk       integer                       ,\n"
          + "    wr_returned_time_sk       integer                       ,\n"
          + "    wr_item_sk                integer               not null,\n"
          + "    wr_refunded_customer_sk   integer                       ,\n"
          + "    wr_refunded_cdemo_sk      integer                       ,\n"
          + "    wr_refunded_hdemo_sk      integer                       ,\n"
          + "    wr_refunded_addr_sk       integer                       ,\n"
          + "    wr_returning_customer_sk  integer                       ,\n"
          + "    wr_returning_cdemo_sk     integer                       ,\n"
          + "    wr_returning_hdemo_sk     integer                       ,\n"
          + "    wr_returning_addr_sk      integer                       ,\n"
          + "    wr_web_page_sk            integer                       ,\n"
          + "    wr_reason_sk              integer                       ,\n"
          + "    wr_order_number           integer               not null,\n"
          + "    wr_return_quantity        integer                       ,\n"
          + "    wr_return_amt             decimal(7,2)                  ,\n"
          + "    wr_return_tax             decimal(7,2)                  ,\n"
          + "    wr_return_amt_inc_tax     decimal(7,2)                  ,\n"
          + "    wr_fee                    decimal(7,2)                  ,\n"
          + "    wr_return_ship_cost       decimal(7,2)                  ,\n"
          + "    wr_refunded_cash          decimal(7,2)                  ,\n"
          + "    wr_reversed_charge        decimal(7,2)                  ,\n"
          + "    wr_account_credit         decimal(7,2)                  ,\n"
          + "    wr_net_loss               decimal(7,2)                  ,\n"
          + "    primary key (wr_item_sk, wr_order_number, wr_returned_date_sk)\n"
          + ")";

  private static String web_sales =
      "create table web_sales\n"
          + "(   \n"
          + "    ws_sold_date_sk           integer                       ,\n"
          + "    ws_sold_time_sk           integer                       ,\n"
          + "    ws_ship_date_sk           integer                       ,\n"
          + "    ws_item_sk                integer               not null,\n"
          + "    ws_bill_customer_sk       integer                       ,\n"
          + "    ws_bill_cdemo_sk          integer                       ,\n"
          + "    ws_bill_hdemo_sk          integer                       ,\n"
          + "    ws_bill_addr_sk           integer                       ,\n"
          + "    ws_ship_customer_sk       integer                       ,\n"
          + "    ws_ship_cdemo_sk          integer                       ,\n"
          + "    ws_ship_hdemo_sk          integer                       ,\n"
          + "    ws_ship_addr_sk           integer                       ,\n"
          + "    ws_web_page_sk            integer                       ,\n"
          + "    ws_web_site_sk            integer                       ,\n"
          + "    ws_ship_mode_sk           integer                       ,\n"
          + "    ws_warehouse_sk           integer                       ,\n"
          + "    ws_promo_sk               integer                       ,\n"
          + "    ws_order_number           integer               not null,\n"
          + "    ws_quantity               integer                       ,\n"
          + "    ws_wholesale_cost         decimal(7,2)                  ,\n"
          + "    ws_list_price             decimal(7,2)                  ,\n"
          + "    ws_sales_price            decimal(7,2)                  ,\n"
          + "    ws_ext_discount_amt       decimal(7,2)                  ,\n"
          + "    ws_ext_sales_price        decimal(7,2)                  ,\n"
          + "    ws_ext_wholesale_cost     decimal(7,2)                  ,\n"
          + "    ws_ext_list_price         decimal(7,2)                  ,\n"
          + "    ws_ext_tax                decimal(7,2)                  ,\n"
          + "    ws_coupon_amt             decimal(7,2)                  ,\n"
          + "    ws_ext_ship_cost          decimal(7,2)                  ,\n"
          + "    ws_net_paid               decimal(7,2)                  ,\n"
          + "    ws_net_paid_inc_tax       decimal(7,2)                  ,\n"
          + "    ws_net_paid_inc_ship      decimal(7,2)                  ,\n"
          + "    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,\n"
          + "    ws_net_profit             decimal(7,2)                  ,\n"
          + "    primary key (ws_item_sk, ws_order_number, ws_sold_date_sk)\n"
          + ")";

  private String db;
  private Connection conn;
  private Statement sm;
  private ResultSet rs;

  private String table;
  private String partitionKey;

  private static final Map<String, String> julianMap = new TreeMap<>();
  private Map<String, String> partitionMap = new HashMap<>();

  @SneakyThrows
  public Partition(Source source) {
    db = source.getDb();
    Class.forName("com.mysql.cj.jdbc.Driver");
    conn =
        DriverManager.getConnection(
            "jdbc:mysql://" + source.getHost() + "/" + source.getDb(),
            source.getUser(),
            source.getPassword());
    sm = conn.createStatement();
    table = source.getTable();
    partitionKey = source.getPartitionKey();
    partitionMap.put("catalog_returns", "cr_returned_date_sk");
    partitionMap.put("catalog_sales", "cs_sold_date_sk");
    partitionMap.put("inventory", "inv_date_sk");
    partitionMap.put("store_returns", "sr_returned_date_sk");
    partitionMap.put("store_sales", "ss_sold_date_sk");
    partitionMap.put("web_returns", "wr_returned_date_sk");
    partitionMap.put("web_sales", "ws_sold_date_sk");
  }

  @SneakyThrows
  @Override
  public void run() {

    sm = conn.createStatement();

    for (Map.Entry<String, String> e : partitionMap.entrySet()) {

      String table = e.getKey();
      String col = e.getValue();
      System.out.println("drop table " + table + ";");

      String partitionResult = "partition by range (" + col + ") (\n";

      // select distinct sr_returned_date_sk from store_returns order by sr_returned_date_sk;
      rs = sm.executeQuery("select distinct " + col + " from " + table + " order by " + col + ";");
      while (rs.next()) {
        String row = rs.getString(1);
        int[] date = Util.toTimestamp(Double.parseDouble(row));
        String value = date[0] + String.valueOf(date[1]);
        julianMap.put(row, value);
      }
      int paritionNum = 500;
      int batchSize = (julianMap.size() / paritionNum) + 1;

      int round = 0;
      int total = 0;
      int pName = 0;
      for (Map.Entry<String, String> entry : julianMap.entrySet()) {

        total++;
        if (total == julianMap.size()) {
          partitionResult += "\tpartition p" + ++pName + " values less than maxvalue);";
          break;
        }
        while (round++ == batchSize) {
          partitionResult +=
              "\t partition p" + pName++ + " values less than (" + entry.getKey() + "),\n";
          round = 0;
        }
      }
      switch (e.getKey()) {
        case "catalog_returns":
          catalog_returns += partitionResult;
          System.out.println(catalog_returns);
          break;
        case "catalog_sales":
          catalog_sales += partitionResult;
          System.out.println(catalog_sales);
          break;
        case "inventory":
          inventory += partitionResult;
          System.out.println(inventory);
          break;
        case "store_returns":
          store_returns += partitionResult;
          System.out.println(store_returns);
          break;
        case "store_sales":
          store_sales += partitionResult;
          System.out.println(store_sales);
          break;
        case "web_returns":
          web_returns += partitionResult;
          System.out.println(web_returns);
          break;
        case "web_sales":
          web_sales += partitionResult;
          System.out.println(web_sales);
          break;
      }
    }
    if (rs != null) {
      rs.close();
    }
    sm.close();
    conn.close();
  }
}
