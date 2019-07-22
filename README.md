# n3-deep6
An opinionated hexastore for linking education data


Work in progress, much more documentation to come, but the main.go in /example/d6 
shows most of the key fetures.

Sample datasets for typical ed-tech data; SIF, XAPI are provided along with
samples of arbitrary json formats that represent the sort of applicaiton data
you might need to integrate.

Deep6 in this repo only really of use to anyone who wants to use an embedded datastore from within
golang. The other layers that add data version control and access via web endpoints (and queries using GraphQL) will be
added in the next couple of weeks.

