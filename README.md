# Engineering Coding Challenge

In this challenge you are required to do some analysis on the datasets provided and write unit tests. Integration tests are a bonus but not required. Python should be the language of choice but you are free to use any other language you are familiar with.

## Credit Card Vendor Mapping 
In later parts of this project you will need this mapping of credit_card_vendor to list of card prefix.

```
# prefix_vendor = list of credit first digits that are representing this vendor.
maestro = ['5018', '5020', '5038', '56##']
mastercard = ['51', '52', '54', '55', '222%']
visa = ['4']
amex = ['34', '37']
discover = ['6011', '65']
diners = ['300', '301', '304', '305', '36', '38']
jcb16 = ['35']
jcb15 = ['2131', '1800']
```

## Part 1

Setup a git project that you can push to Github or other provider of your choice. We would like to see clean code and a well structured project using a framework of your choice. You will also need to provide a working set of functions or function, that we can execute to reproduce your results.  

## Part 2

Read fraud.zip and store the data into an SQL database.

## Part 3

Sanitise the data of both transaction-001.zip and transaction-002.zip by removing transactions where column `credit_card_number` is not part of the previous provided list.

**Example**: a credit card that starts with `98` is not a valid card, it should be discarded from the sanitised dataset.
 
## Part 4

- Going forward, only the sanitised dataset should be used
- Find in the sanitised dataset if it contains fraudulent transactions (from fraud.zip) and report their number
- Create a report of the number of fraudulent transactions per state
- Create a report of the number of fraudulent transactions per card vendor, eg: maestro => 45, amex => 78, etc..
- Create a report of the number of fraudulent transactions per card vendor, eg: maestro => 45, amex => 78, etc..
- Create a dataset of 3 columns and save in both JSON and in a binary file format that you believe it's suitable for BI analysis:
  - column 1: masked credit card: replace 9 last digits of the credit card with `*`
  - column 2: ip address
  - column 3: state
  - column 4: sum of number of byte of (column 1 + column 2 + column 3)
