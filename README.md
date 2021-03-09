# n3-deep6
An opinionated hexastore for linking education data

Update Nov. 2020
Deep-6 has been in use on multiple projects for over a year now, and we're pretty happy with it.
The main documentaion can actually be found at https://github.com/nsip/n3 since the db is
only one layer of the n3 solution - from that link you can download prebuilt binaries
that include the crdt and GraphQL layers, all hosted in a web service.

More documentation available (see above), but the main.go in /example/d6 shows most of the key fetures using this as a pure golang embedded db.

Sample datasets for typical ed-tech data; SIF, XAPI are provided along with
samples of arbitrary json formats that represent the sort of application data
you might need to integrate.

Deep6 in this repo only really of use to anyone who wants to use an embedded datastore from within
golang. The other layers that add data version control and access via web endpoints (and queries using GraphQL) will be
added in the next couple of weeks.

To build and run a demo, go to /example/d6

then go build as normal, to run use:

`>./clean.sh && ./d6 > out.txt`


running the clean script is optional it just removes any existing database files, the output of d6 is piped to a file simply because it's verbose (multiple queries with annotations), so is easier to read in a file.


