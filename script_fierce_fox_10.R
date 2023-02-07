
set.seed(2022) 
library(tidyverse)
library(dplyr)
library(lubridate)
# Model
library(caret)
library(xgboost)
library(data.table)
library(mltools)

# ##### PREPROCESSING #####

# --- Load CSV Files ---
classification = read_csv("classification.csv")
customers = read_csv("customers.csv") 
sales_orders_header = read_csv("sales_orders_header.csv") 
sales_orders = read_csv("sales_orders.csv")
business_units = read_csv("business_units.csv") 
service_map = read_csv("service_map.csv")

# --- Drop unnecessary tables and columns  ---
# Service_map doesn't add any information that is not already in sales-orders.csv -> delete table
remove(service_map)

# In business_units, the YHKOKRS-value is always 5180. It doesn't add any information -> drop YHKOKRS-column
business_units = business_units %>% select(-YHKOKRS)

# --- Fix types of all columns ---
# set types according to specification in Challenge_Description.pdf
# character types -> factor

# Classification
classification = classification %>% mutate(
  Reseller = as.integer(Reseller),
  Test_set_id = as.integer(Test_set_id)
)
# Customers
customers = customers %>% mutate(
  Type = as.factor(Type)
)
# Sales_Orders_Header
sales_orders_header = sales_orders_header %>% mutate(
  Sales_Organization = as.factor(Sales_Organization),
  Creation_Date = as.Date(Creation_Date),
  Creator = as.numeric(as.factor(sales_orders_header$Creator)),
  Document_Type = as.factor(Document_Type),
  Release_Date = as.Date(Release_Date),
  Delivery = as.factor(Delivery)
)
# Sales_Orders
sales_orders = sales_orders %>% mutate(
  Item_Position = as.character(Item_Position),
  Num_Items = as.integer(Num_Items),
  Material_Class = as.integer(Material_Class),
  # Cost_Center = as.factor(Cost_Center), # Don't turn Cost_Center into a factor now, because it will be used as a key in a merge later
)
# Business_Units
business_units = business_units %>% mutate(
  # Cost_Center = as.factor(Cost_Center) # Don't turn Cost_Center into a factor now, because it will be used as a key in a merge later
  Business_Unit = as.factor(Business_Unit)
)


# --- Join all customers into sales_orders ---
# join customers and sales_orders on the Sales_Order-column
# join customers and sales_orders on Item_Position and Sales_Order
# For non-matching orders set Item_Position to 0 and match again on item_position and sales_order

# Customers who matched with order and item position
matched_customers <- semi_join(customers, sales_orders, by=c("Item_Position"="Item_Position", "Sales_Order"="Sales_Order"))

# Sales Orders that matched with a customer
matched_sales_orders <- semi_join(sales_orders, customers, by=c("Item_Position"="Item_Position", "Sales_Order"="Sales_Order"))

# Customers that didn’t match with order and item position
remaining_customers <- anti_join(customers, sales_orders, by=c('Item_Position'= 'Item_Position', 'Sales_Order' = 'Sales_Order'))

# Sales Order that didn’t match with a customer
remaining_sales_orders <- anti_join(sales_orders, customers, by=c("Item_Position"="Item_Position", "Sales_Order"="Sales_Order"))
remaining_customers <- remaining_customers %>% mutate(Item_Position="0") 
remaining_sales_orders <- remaining_sales_orders %>% mutate(Item_Position="0")
clean_customers <- bind_rows(matched_customers, remaining_customers) 
clean_sales_orders <- bind_rows(matched_sales_orders, remaining_sales_orders)

# --- Join all(remaining) tables into sales-order.csv with left joins ---

# Join classification into customers
customers_classification <- left_join(clean_customers, classification, by = c('Customer_ID' = 'Customer_ID'))
# No value in Sales_Order, Item_Position, Type and Customer_ID is NA -> that's good

# Join customers_classification to clean sales
sales <- left_join(clean_sales_orders, customers_classification, by = c('Sales_Order', 'Item_Position'))

#df <- sales_orders_header$Creation_Date - sales_orders_header$Release_Date
#df <- filter(df, x>0)

# Check if dates are equal
# sales_orders_header$Creation_Date == sales_orders_header$Release_Date
# This outputs only a few values not being equal

# Join sale order headers to sale order
sales <- left_join(sales, sales_orders_header, by = c('Sales_Order' = 'Sales_Order'))
# rename columns Net_Value.x and Net_Value.y
sales <- sales %>% rename(Net_Value = Net_Value.x, Net_Value_Header = Net_Value.y)

# 43 rows hold NAs after this step for the following columns:
# less than 0,5% of data for the affected customers - sum(sales$Customer_ID == xx)
sales <- sales[!is.na(sales$Document_Type), ]

# Sales order with corresponding business unit - join business units to clean sales orders
sales <- left_join(sales, business_units, by = c('Cost_Center' = 'Cost_Center'))

# Drop sales order and item position
# sales <- sales %>% select(-Sales_Order, -Item_Position) 

#sales$Cost_Center = as.numeric(as.factor(sales$Cost_Center))

# --- Turn all hex values (starting with 0x) into easier readable numeric factors ---
sales <- sales %>% mutate(
  Material_Code = as.numeric(as.factor(Material_Code)),
  Cost_Center = as.numeric(as.factor(Cost_Center)),
  Customer_ID = as.numeric(as.factor(Customer_ID))
)

# Customer 46 still has 2 rows where material class is NA, but has 133 entries in total
# -> remove the NAs
sales <- sales[!is.na(sales$Material_Class), ]


# ##### AGGREGATION #####
### Start of aggregation / feature creation ###

## Number of orders of each Customer
aggregate <- sales %>% group_by(Customer_ID) %>% summarize(Number_of_Orders = n())

## Label of each Customer
label_of_Customer <- sales %>% select(Customer_ID, Reseller) %>% distinct()
aggregate <- left_join(aggregate, label_of_Customer, by = c('Customer_ID' = 'Customer_ID'))

## Date differences between Creation Date and Release Date - 
# New row holding the difference between Release and Creation Date
sales$DiffDate <- as.numeric(difftime(sales$Release_Date, sales$Creation_Date, units = "days"))
sales$DiffDate[is.na(sales$DiffDate)] <- 0

# Customer_ID mapped to the sum of all DiffTimes of his orders
temp <- sales %>% group_by(Customer_ID) %>% summarize(DiffDate = n())
aggregate <- left_join(aggregate, temp, by = c('Customer_ID' = 'Customer_ID'))

# sales grouped by Customer_ID
sales_by_Customer <- sales %>% group_by(Customer_ID)

# Minimum and Maximum of Net_Value of sales_order of customer
min_max_Net_Value <- sales_by_Customer %>% summarise(min_Net_Value = min(Net_Value), max_Net_Value = max(Net_Value))
aggregate <- left_join(aggregate, min_max_Net_Value, by = c('Customer_ID' = 'Customer_ID'))

# Sum of all Net_Value for each customer
sum_Net_Value = sales_by_Customer %>%  summarise(sum_Net_Value = sum(Net_Value))
aggregate <- left_join(aggregate, min_max_Net_Value, by = c('Customer_ID' = 'Customer_ID'))

# Count of Business_Units BU_A, BU_B, BU_C, BU_D
count_Business_Unit = sales_by_Customer %>% summarise(BU_A = sum(Business_Unit == "BU_A"), BU_B = sum(Business_Unit == "BU_B"), BU_C = sum(Business_Unit == "BU_C"), BU_D = sum(Business_Unit == "BU_D"))
aggregate <- left_join(aggregate, count_Business_Unit, by = c('Customer_ID' = 'Customer_ID'))

# average number of Items per salesorder 
avg_Num_Items = sales_by_Customer %>% summarise(avg_Num_Items = sum(Num_Items) / n_distinct(Sales_Order)) 
aggregate <- left_join(aggregate, avg_Num_Items, by = c('Customer_ID' = 'Customer_ID'))

# total number number of Items
tot_Num_Items = sales_by_Customer %>% summarise(tot_Num_Items = sum(Num_Items)) 
aggregate <- left_join(aggregate, tot_Num_Items, by = c('Customer_ID' = 'Customer_ID'))

# visits per Cost_Center
visits_per_costcenter = sales %>% group_by(Customer_ID, Cost_Center) %>% summarise(visits = n ()) %>% spread(Cost_Center, visits, fill = 0)
aggregate <- left_join(aggregate, visits_per_costcenter, by = c('Customer_ID' = 'Customer_ID'))

# count of type of Sales_Order: SOP – Shipped to Party, STP – Sold to Party
type = sales_by_Customer %>% summarise(type_SOP = sum(Type == "SOP"), type_STP = sum(Type == "STP"))
aggregate <- left_join(aggregate, type, by = c('Customer_ID' = 'Customer_ID'))

# count of different delivery_status per customer: Completely processed, Not relevant, Not yet processed, Partially processed
delivery = sales_by_Customer  %>% summarise(Delivery_cp = sum(Delivery == "Completely processed"), Delivery_nr = sum(Delivery == "Not relevant"), Delivery_nyp = sum(Delivery == "Not yet processed"), Delivery_pp = sum(Delivery == "Partially processed"))
aggregate <- left_join(aggregate, delivery, by = c('Customer_ID' = 'Customer_ID'))

# visits per creator
fullfillment_per_creator = sales %>% group_by(Customer_ID, Creator) %>% summarise(fullfilled = n ()) %>% spread(Creator, fullfilled, fill = 0)
aggregate <- left_join(aggregate, fullfillment_per_creator, by = c('Customer_ID' = 'Customer_ID'))

# 
# --- Categorical features: cannot be used in the current model ---

# Most common Material_Class for each customer
#common_Material_Class = sales_by_Customer %>% summarise(common_Material_Class = names(which.max(table(Material_Class))))
#aggregate <- left_join(aggregate, common_Material_Class, by = c('Customer_ID' = 'Customer_ID'))

# Most common Material_Code for each customer
#common_Material_Code = sales_by_Customer %>% summarise(common_Material_Code = names(which.max(table(Material_Code))))
#aggregate <- left_join(aggregate, common_Material_Code, by = c('Customer_ID' = 'Customer_ID'))

# Most common Pair of Material_Class and Material_Code for each customer
#common_Material_Class_Code = sales_by_Customer %>%  summarise(common_Material_Class_Code = names(which.max(table(interaction(Material_Class, Material_Code)))))
#aggregate <- left_join(aggregate, common_Material_Class_Code, by = c('Customer_ID' = 'Customer_ID'))

# Most common Cost_Center for each customer
#common_Cost_Center = sales_by_Customer %>% summarise(common_Cost_Center = names(which.max(table(Cost_Center))))
#aggregate <- left_join(aggregate, common_Cost_Center, by = c('Customer_ID' = 'Customer_ID'))

# Most common Business_Unit
# common_Business_Unit = sales_by_Customer %>% summarise(common_Business_Unit = names(which.max(table(Business_Unit))))
# aggregate <- left_join(aggregate, common_Business_Unit, by = c('Customer_ID' = 'Customer_ID'))

# Most common Pair of Cost_Center and Business_Unit for each customer
# common_Cost_Center_Business_Unit = sales_by_Customer %>%  summarise(common_Cost_Center_Business_Unit = names(which.max(table(interaction(Cost_Center, Business_Unit)))))
# aggregate <- left_join(aggregate, common_Cost_Center_Business_Unit, by = c('Customer_ID' = 'Customer_ID'))

# --- End of aggregation ---

# ##### MODEL #####

# 1) feature engineering / aggregation
#     - group table by customer_id, aggregate features, create new features
# 2) create train/validation/test split of the data
# 3) train XGboost model to predict if customers were resellers

# 2 - train/test split
training_dataset = aggregate[!is.na(aggregate$Reseller),]
test_dataset = aggregate[is.na(aggregate$Reseller),]

# 3 - train/validation split
trainIndex = createDataPartition(training_dataset$Reseller, p=0.8, list=FALSE, times=1)
training_dataset = training_dataset[trainIndex,]
validation_dataset  = training_dataset[-trainIndex,]

# 4 - Split into features and labels
X_train = training_dataset %>% select(-Customer_ID, -Reseller)
X_validation = validation_dataset %>% select(-Customer_ID, -Reseller) # -Test_set_id, weggemacht
X_test = test_dataset %>% select(-Customer_ID, -Reseller)
y_train = training_dataset %>% select(Reseller)
y_validation = validation_dataset %>% select(Reseller)
y_test = test_dataset %>% select(Reseller)

dtrain = xgb.DMatrix(data=as.matrix(X_train), label=as.matrix(y_train))
dvalidation = xgb.DMatrix(data=as.matrix(X_validation), label=as.matrix(y_validation))
watchlist <- list(train=dtrain, validation=dvalidation)
bstSparse <- xgb.train(data = dtrain, max.depth = 100, eta = 0.1, nthread = 2, nrounds = 31, min_child_weight=0.3, watchlist = watchlist, objective = "binary:logistic")

predicted_y_validation = predict(bstSparse, as.matrix(X_validation))
confusionMatrix(as.factor(y_validation$Reseller), as.factor(round(predicted_y_validation)))

# prediction_not_aggregated = tibble(Customer_ID=validation_dataset$Customer_ID, prediction=predicted_y_validation, Reseller=validation_dataset$Reseller)
# prediction_aggregated = prediction_not_aggregated %>% group_by(Customer_ID) %>% summarise(Customer_ID = max(Customer_ID), prediction=round(mean(prediction)), Reseller=mean(Reseller))
# confusionMatrix(data=as.factor(prediction_aggregated$Reseller), reference=as.factor(prediction_aggregated$prediction))

#############



predicted_y_test = predict(bstSparse, as.matrix(X_test))

test_prediction_aggregated = tibble(Customer_ID=test_dataset$Customer_ID, prediction=predicted_y_test)

classification$Customer_ID = as.numeric(as.factor(classification$Customer_ID))

test_prediction_aggregated <- left_join(test_prediction_aggregated, classification, by = c("Customer_ID" = "Customer_ID"))
# Test_set_id=test_dataset$Test_set_id ,
# test_prediction_aggregated = test_prediction_not_aggregated %>% group_by(Customer_ID) %>% summarise(id=max(Test_set_id),prediction=round(mean(prediction)))
test_prediction_aggregated = test_prediction_aggregated %>% select(Test_set_id, prediction)
test_prediction_aggregated = test_prediction_aggregated %>% arrange(Test_set_id)

test_prediction_aggregated = round(test_prediction_aggregated)
test_prediction_aggregated = rename(test_prediction_aggregated, id = Test_set_id)

write_csv(test_prediction_aggregated, "predictions_fierce_fox_10.csv")
