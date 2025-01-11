# How to install: 

cd to the base repo, optionally create and activate a venv for your project. Then run 

pip install .

After this you should be able to run the code just be executing: 

extract-data 

On the command line. 

If you are going to be making edits, or running tests instead run in an editable install and install the dev dependencies: 

pip install -e .[dev]

# Engineering Coding Challenge

In this challenge you are required to do some analysis on the datasets provided and write unit tests. The preferred languages are Python and SQL, and you are welcome to use any libraries or frameworks you are comfortable with. You are expected to complete parts 1, 2, 3 and 4 at home. Part 5 will be completed during the face to face interview process.

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

Read `fraud.zip` and store the data into a data structure of your choice or even a local SQL database if you prefer.

## Part 3

Sanitise the data of both `transaction-001.zip` and `transaction-002.zip` by removing transactions where column `credit_card_number` is not part of the previous provided list. Going forward, only the sanitised dataset should be used.

**Example**: a credit card that starts with `98` is not a valid card, it should be discarded from the sanitised dataset.
 
## Part 4

Find the fraudulent transactions (from `fraud.zip`) in the sanitised dataset and report the total.

## Part 5

You are required to prepare your code, so that the below tasks can be completed during the face to face interview process:

- Create a report of the number of fraudulent transactions per state
- Create a report of the number of fraudulent transactions per card vendor, eg: maestro => 45, amex => 78, etc..
- Create a dataset of 3 columns and save in both `JSON` and in a binary file format that you believe it's suitable for BI analysis:
  - column 1: masked credit card: replace 9 last digits of the credit card with `*`
  - column 2: ip address
  - column 3: state
  - column 4: sum of number of byte of (column 1 + column 2 + column 3)
- Optionally add unit tests
