Scala Distributed Sorting
===
This project is for POSTECH CS490 course. (2016 Fall Semester)

## Design
For the design documentations, check [here](Design). Download and open them with your browser.

## Code
For state implementation(the most important part), see [master](src/main/scala/dsorting/states/master) and [slave](src/main/scala/dsorting/states/slave) directory.

If you want to see how the message handling is implemented, check [this file](src/main/scala/dsorting/messaging/Messaging.scala).

Tests are [here](src/test/scala/dsorting).
