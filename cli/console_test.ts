import { expect } from "chai";

import { formatExecutionSuffix } from "df/cli/console";
import { suite, test } from "df/testing";

suite('format execution suffix', () => {
    test('format execution suffix', () => {
        expect(formatExecutionSuffix([], [])).deep.equals('');
        expect(formatExecutionSuffix(["dataform-915a03fe1"], [])).deep.equals(" (jobId: dataform-915a03fe1)");
        expect(formatExecutionSuffix([], ["10 MiB"])).deep.equals(" (Bytes billed: 10 MiB)");
        expect(formatExecutionSuffix(["dataform-915a03fe1"], ["17 KiB"])).deep.equals(" (jobId: dataform-915a03fe1 | Bytes billed: 17 KiB)");
        expect(formatExecutionSuffix(["dataform-915a03fe1", "dataform-915a03fe2"], ["17 KiB", "1 GiB"])).deep.equals(" (jobId: dataform-915a03fe1, dataform-915a03fe2 | Bytes billed: 17 KiB, 1 GiB)");

    });
});