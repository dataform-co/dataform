import { expect } from "chai";

import { formatBytesInHumanReadableFormat, formatExecutionSuffix } from "df/cli/util";
import { suite, test } from "df/testing";

suite('format execution suffix', () => {
    test('format execution suffix', () => {
        expect(formatExecutionSuffix([], [])).deep.equals('');
        expect(formatExecutionSuffix(["dataform-915a03fe1"], [])).deep.equals(" \n \t jobId: dataform-915a03fe1");
        expect(formatExecutionSuffix([], ["10 MiB"])).deep.equals(" \n \t Bytes billed: 10 MiB");
        expect(formatExecutionSuffix(["dataform-915a03fe1"], ["17 KiB"])).deep.equals(" \n \t jobId: dataform-915a03fe1,\n \t Bytes billed: 17 KiB");
        expect(formatExecutionSuffix(["dataform-915a03fe1", "dataform-915a03fe2"], ["17 KiB", "1 GiB"])).deep.equals(" \n \t jobId: dataform-915a03fe1, dataform-915a03fe2,\n \t Bytes billed: 17 KiB, 1 GiB");
    });
});

suite('format bytes in human readable format', () => {
    test('format bytes in human readable format', () => {
        expect(formatBytesInHumanReadableFormat(-1)).deep.equals('0 B');
        expect(formatBytesInHumanReadableFormat(0)).deep.equals('0 B');
        expect(formatBytesInHumanReadableFormat(1)).deep.equals('1.00 B');
        expect(formatBytesInHumanReadableFormat(500)).deep.equals('500.00 B');
        expect(formatBytesInHumanReadableFormat(1024)).deep.equals('1.00 KiB');
        expect(formatBytesInHumanReadableFormat(1500)).deep.equals('1.46 KiB');
        expect(formatBytesInHumanReadableFormat(1048576)).deep.equals('1.00 MiB');
        expect(formatBytesInHumanReadableFormat(1024 * 1024 * 1.5)).deep.equals('1.50 MiB');
        expect(formatBytesInHumanReadableFormat(1073741824)).deep.equals('1.00 GiB');
        expect(formatBytesInHumanReadableFormat(1099511627776)).deep.equals('1.00 TiB');
        expect(formatBytesInHumanReadableFormat(1125899906842624)).deep.equals('1.00 PiB');
    });
});

