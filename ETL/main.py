from utils.QueryExecutor import QueryExecutorSource, InsertExecutorTarget, QueryExecutorTarget
from datetime import datetime
import multiprocessing

class Main():
    def __init__ (self):
        return

    def Load_Target_Tables(self):
        processes = []

        process1 = multiprocessing.Process(target = self.Load_Sales_Summary, name = 'Sales Summary')
        process2 = multiprocessing.Process(target = self.Load_Customer_Summary, name = 'Customer Summary')
        process3 = multiprocessing.Process(target = self.Load_Product_Summary, name = 'Product Summary')
        processes.append(process1)
        processes.append(process2)
        processes.append(process3)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

    def Load_Sales_Summary(self):
        print('LOADING SALES SUMMARY')
        sales_summary = QueryExecutorSource('EXEC SALES_SUMMARY')
        # FILL NULL VALUES FOR DATE
        sales_summary['ORDER_DATE'] = sales_summary['ORDER_DATE'].fillna(datetime.today())
        InsertExecutorTarget(data = sales_summary, table_name = 'SALESUMMARY')
    
    def Load_Customer_Summary(self):
        print('LOADING CUSTOMER SUMMARY')
        customer_summary = QueryExecutorSource('EXEC CUSTOMER_SUMMARY')
        # FILL NULL VALUES FOR DATE
        customer_summary['SIGNUP_DATE'] = customer_summary['SIGNUP_DATE'].fillna(datetime.today())
        InsertExecutorTarget(data = customer_summary, table_name = 'CUSTOMERSUMMARY')


    def Load_Product_Summary(self):
        print('LOADING PRODUCTS SUMMARY')
        product_summary = QueryExecutorSource('EXEC PRODUCT_SUMMARY')
        InsertExecutorTarget(data = product_summary, table_name = 'PRODUCTSUMMARY')


if __name__ == '__main__':
    Main().Load_Target_Tables()