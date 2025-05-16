
import { expect } from "chai";

import { formatBytesInHumanReadableFormat } from "df/core/utils";
import { suite, test } from "df/testing";

suite('format bytes from dry run in human readable format', () => {
    test('format bytes from dry run in human readable format', () => {
        expect(formatBytesInHumanReadableFormat(0)).deep.equals('0 B');
        expect(formatBytesInHumanReadableFormat(1)).deep.equals('1.00 B');
        expect(formatBytesInHumanReadableFormat(500)).deep.equals('500.00 B');
        expect(formatBytesInHumanReadableFormat(1024)).deep.equals('1.00 KB');
        expect(formatBytesInHumanReadableFormat(1500)).deep.equals('1.46 KB');
        expect(formatBytesInHumanReadableFormat(1048576)).deep.equals('1.00 MB');
        expect(formatBytesInHumanReadableFormat(1024 * 1024 * 1.5)).deep.equals('1.50 MB');
        expect(formatBytesInHumanReadableFormat(1073741824)).deep.equals('1.00 GB');
        expect(formatBytesInHumanReadableFormat(1099511627776)).deep.equals('1.00 TB');
        expect(formatBytesInHumanReadableFormat(1125899906842624)).deep.equals('1.00 PB');
    });
});
