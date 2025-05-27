
import { expect } from "chai";

import { formatBytesInHumanReadableFormat } from "df/core/utils";
import { suite, test } from "df/testing";

suite('format bytes in human readable format', () => {
    test('format bytes in human readable format', () => {
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
