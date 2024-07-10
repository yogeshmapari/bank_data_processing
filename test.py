list1=["customers",
"accounts",
"transactions",
"loans",
"cards",
"branches",
"employees",
"atms",
"investments",
"customer_support",
"fixed_deposits",
"recurring_deposits",
"online_banking",
"bill_payments",
"insurance",
"credit_scores",
"service_charges",
"cheques",
"savings_goals",
"mortgage_applications"]
for i in list1:
    with open('dlt_models/dlt_load_'+i+'.py','w'):
        pass