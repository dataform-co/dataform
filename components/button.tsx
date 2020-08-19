import { AnchorButton } from "@blueprintjs/core";
import { IButtonProps } from "@blueprintjs/core/lib/esm/components/button/abstractButton";
import * as React from "react";

type StylizedIcon = "schedule" | "assertion" | "documentation";

export interface IStylizedButtonProps extends IButtonProps {
  stylizedIcon: StylizedIcon;
}

export function StylizedAnchorButton({ stylizedIcon, ...rest }: IStylizedButtonProps) {
  return <AnchorButton rightIcon={chooseIcon(stylizedIcon)} {...rest} />;
}

function chooseIcon(stylizedIcon: StylizedIcon) {
  switch (stylizedIcon) {
    case "schedule":
      return <SchedulesIcon />;
    case "assertion":
      return <AssertionsIcon />;
    case "documentation":
      return <DocumentationIcon />;
  }
}

function SchedulesIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="10" cy="10" r="10" fill="#5E73E5" />
      <path
        d="M4.375 15.4217C3.02053 14.0167 2.1875 12.1057 2.1875 10C2.1875 7.6238 3.24834 5.49538 4.92225 4.0625C6.28785 2.89354 8.06147 2.1875 10 2.1875C11.9385 2.1875 13.7122 2.89354 15.0777 4.0625C16.7517 5.49538 17.8125 7.6238 17.8125 10C17.8125 12.1057 16.9795 14.0167 15.625 15.4217L10 10L4.375 15.4217Z"
        fill="#2F419B"
      />
      <path
        d="M18.2789 12.2167L9.88594 11.8927C8.66355 11.8455 7.80124 10.6757 8.11785 9.49403C8.43447 8.31242 9.76617 7.73046 10.8484 8.30079L18.2789 12.2167Z"
        fill="white"
      />
      <circle cx="10" cy="10" r="0.9375" fill="#5E73E5" />
    </svg>
  );
}

function AssertionsIcon() {
  return (
    <svg width="26" height="26" viewBox="0 0 26 26" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path
        d="M20.0002 0H12.0669C8.75319 0 6.06689 2.68629 6.06689 6V13.9333C6.06689 17.247 8.75318 19.9333 12.0669 19.9333h10.0002C23.3139 19.9333 26.0002 17.247 26.0002 13.9333V6C26.0002 2.68629 23.3139 0 20.0002 0Z"
        fill="#5E73E5"
      />
      <path
        d="M6.13333 13.8666H6C2.68629 13.8666 0 16.5529 0 19.8666V20C0 23.3137 2.68629 26 6 26H6.13333C9.44704 26 12.1333 23.3137 12.1333 20V19.8666C12.1333 16.5529 9.44704 13.8666 6.13333 13.8666Z"
        fill="#2F419B"
      />
      <path
        d="M14.2998 11.9889L20.6554 5.63336"
        stroke="white"
        strokeWidth="2"
        strokeMiterlimit="10"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M16.9004 5.63336h10.8004V9.53336"
        stroke="white"
        strokeWidth="2"
        strokeMiterlimit="10"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function DocumentationIcon() {
  return (
    <svg width="20" height="23" viewBox="0 0 20 23" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M13.1429 21.4286H4C1.79086 21.4286 0 19.6377 0 17.4286V4C0 1.79086 1.79086 0 4 0H12.1429L17.1429 5V17.4286C17.1429 19.6377 15.352 21.4286 13.1429 21.4286ZM5.28548 5.14286C4.73319 5.14286 4.28548 5.59058 4.28548 6.14286C4.28548 6.69515 4.73319 7.14286 5.28548 7.14286H8.28548C8.83776 7.14286 9.28548 6.69515 9.28548 6.14286C9.28548 5.59058 8.83776 5.14286 8.28548 5.14286H5.28548Z"
        fill="#5E73E5"
      />
      <path d="M12.1426 5V0L17.1426 5H12.1426Z" fill="#2F419B" />
      <line
        x1="5.28564"
        y1="9.71429"
        x2="11.8571"
        y2="9.71429"
        stroke="#2F419B"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <line
        x1="5.28564"
        y1="13.2856"
        x2="8.28564"
        y2="13.2856"
        stroke="#2F419B"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx="15" cy="17.1429" r="5" fill="#2F419B" />
      <path
        d="M13.5718 17.3214L14.8681 18.5714L17.1432 16.4286"
        stroke="white"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
