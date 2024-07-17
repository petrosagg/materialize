# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.input_data.test_input_operations import (
    ConsistencyTestOperationsInput,
)
from materialize.output_consistency.input_data.test_input_types import (
    ConsistencyTestTypesInput,
)
from materialize.output_consistency.query.query_template import QueryTemplate


class ConsistencyTestInputData:
    """Provides input data for the test execution"""

    def __init__(
        self,
    ) -> None:
        self.types_input = ConsistencyTestTypesInput()
        self.operations_input = ConsistencyTestOperationsInput()
        self.predefined_queries: list[QueryTemplate] = []

    def remove_postgres_incompatible_data(self) -> None:
        self.types_input.remove_postgres_incompatible_data()
        self.operations_input.remove_postgres_incompatible_data()

    def get_stats(self) -> str:
        return (
            f"Input stats:"
            f" count_data_types={self.count_available_data_types()},"
            f" count_ops={self.count_available_ops()},"
            f" count_predefined_queries={self.count_predefined_queries()}"
        )

    def count_available_data_types(self) -> int:
        return len(self.types_input.all_data_types_with_values)

    def count_available_ops(self) -> int:
        return len(self.operations_input.all_operation_types)

    def count_predefined_queries(self) -> int:
        return len(self.predefined_queries)
