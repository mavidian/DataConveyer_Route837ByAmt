# DataConveyer_Route837ByAmt

DataConveyer_Route837ByAmt is a console application to demonstrate routing capabilities of Data Conveyer.

The input file is expected to be a complete X12 interchange envelope that contains EDI 837 transactions, i.e.
medical claims. Each transaction is evaluated by Data Conveyer, specifically the CLM02 element ("claim
monetary amount") is examined and depending on whether it is below $1,000 or not, the transaction gets
routed to one of the 2 output files (_low or _high). Each of the 2 output file contains a complete, 
properly populated X12 interchange envelope.

## Installation

* Fork this repository and clone it onto your local machine, or

* Download this repository onto your local machine.

## Usage

1. Open DataConveyer_Route837ByAmt solution in Visual Studio.

2. Build and run the application, e.g. hit F5

    - a console window with directions will show up.

3. Copy an input file (e.g. Sample837.x12 from ...\Data folder) into the ...Data\In folder

    - the file will get processed as reported in the console window.

4. Review the contents of the output files placed in the ...Data\Out folder.

5. (optional) Repeat steps 3-4 for other additional input file(s).

6. To exit application, hit Enter key into the console window.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)

## Copyright

```
Copyright © 2019-2020 Mavidian Technologies Limited Liability Company. All Rights Reserved.
```
