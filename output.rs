 Or(
  And(Condition(Condition { column_name: "age", threshold: 65.0, comparison: GreaterThanOrEqual }), Or(Condition(Condition { column_name: "age", threshold: 18.0, comparison: LessThan }), Condition(Condition { column_name: "age"
, threshold: 17.0, comparison: LessThanOrEqual }))), Condition(Condition { column_name: "age", threshold: 20.0, comparison: Equal }))

