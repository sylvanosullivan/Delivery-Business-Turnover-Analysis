# Delivery Business Turnover Analysis


In this project, I analyzed data from a commission-based delivery business to predict employee turnover.

### General methodology:


"Turnover" or "churn" target was set at one month. If an employee was seen to have a differential between their first and last completed orders that was less than one month, the employee was considered "churned".

The goal was to develop a tool which would alert management to employees at risk of leaving the company.

For the scope of this project, I limited it to judging whether or not an employee will leave within one month of being hired, which is approximately when many new hires depart.

Turnover was predicted almost entirely with order history - demographic data aside from approximate age was not included (and age was not found to be a significant factor).  Therefor, turnover prediction was based on an __employee satisfaction score__. This was important, because I wanted to focus on factors which the business could control to better retain employees - namely, the average quality of jobs the employee was tasked with. 



### Notes on the project, as publicly presented:

Because the data was procured from a private source, this project cannot be fully presented publicly. __The data cannot be shared.__ Therefor, the presentation Jupyter notebook can only include my original code and some vaguely descriptive graphs.

The rest ofmy original code can be found in src.
