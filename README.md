# Project1_Flink_2018

A simple, simulated, real-time traffic analysis using Apache Flink

## Description of the project

Drivers, fleet owners, transport operations, insurance companies are stakeholders of vehicle monitoring applications which need to have analytical reporting on the mobility patterns of their vehicles, as well as real-time views in order to support quick and efficient decisions towards eco-friendly moves, cost-effective maintenance of vehicles, improved navigation, safety and adaptive risk management. Vehicle sensors do continuously provide data, while on-the-move, they are processed in order to provide valuable information to stakeholders. Applications identify speed violations, abnormal driver behaviors, and/or other extraordinary vehicle conditions, produce statistics per driver/vehicle/fleet/trip, correlate events with map positions and route, assist navigation, monitor fuel consumptions, and perform many other reporting and alerting functions.

## Data

each vehicle reports a position event every 30 seconds with the following format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

- __Time__ a timestamp (integer) in seconds identifying the time at which the position event was emitted
- __VID__ is an integer that identifies the vehicle
- __Spd__ (0 - 100) is an integer that represents the speed mph (miles per hour) of the vehicle
- __XWay__ (0 . . .L−1) identifies the highway from which the position report is emitted
- __Lane__ (0 . . . 4) identifies the lane of the highway from which the position report is emitted (0 if it is an entrance ramp (ENTRY), 1 − 3 if it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT))
- __Dir__ (0 . . . 1) indicates the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling
- __Seg__ (0 . . . 99) identifies the segment from which the position report is emitted
- __Pos__ (0 . . . 527999) identifies the horizontal position of the vehicle as the number of meters from the westernmost point on the highway (i.e., Pos = x)

## Query

- __speedfines__, cars that overcome the speed limit of 90 mph has to be stored in the following format:Time, VID, XWay, Seg, Dir, Spd
- __avgspeedfines__, cars with an average speed higher than 60 mph between segments 52 and 56. Cars are stored in the following format: Time1, Time2, VID, XWay, Dir, AvgSpd, where Time1 is the time of the first event of the segment and Time2 is the time of the last event of the segment.
- __accidents__, detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position. Cars are stored in the following format: Time1, Time2, VID, XWay, Seg, Dir, Pos, where Time1 is the time of the first event the car stops and Time2 is the time of the fourth event the car reports to be stopped.

## Authors
- @matteoanelli
- @damianodallapiccola
