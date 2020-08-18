import * as React from "react";

export interface IStep {
  id: number;
}

export interface IStepIndicatorProps {
  steps: IStep[];
  currentStep: string | number;
  setStep: (id: number) => void;
}

export function StepIndicator({ steps, currentStep, setStep }: IStepIndicatorProps) {
  return (
    <>
      {steps.map(({ id }) => (
        <a onClick={() => setStep(id)} key={`stepIndicator${id}`}>
          {currentStep === id ? <StepIconActive /> : <StepIconInactive />}
        </a>
      ))}
    </>
  );
}

function StepIconInactive() {
  return (
    <div style={{ height: "40px", width: "40px" }}>
      <svg width="6" height="6" viewBox="0 0 6 6" fill="none" xmlns="http://www.w3.org/2000/svg">
        <circle r="3" transform="matrix(-1 0 0 1 3 3)" fill="#F3F3F3" />
        <circle r="1.8" transform="matrix(-1 0 0 1 2.9998 3.00001)" fill="#E0E0E0" />
      </svg>
    </div>
  );
}

function StepIconActive() {
  return (
    <div style={{ height: "40px", width: "40px" }}>
      <svg width="6" height="6" viewBox="0 0 6 6" fill="none" xmlns="http://www.w3.org/2000/svg">
        <circle r="3" transform="matrix(-1 0 0 1 3 3)" fill="#F3F3F3" />
        <circle r="1.8" transform="matrix(-1 0 0 1 2.9998 3.00001)" fill="#4F4F4F" />
      </svg>
    </div>
  );
}
