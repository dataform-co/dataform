import * as React from "react";

import * as styles from "df/components/step_indicator.css";

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
      <div className={styles.stepIndicatorHolder}>
        {steps.map(({ id }) => (
          <a onClick={() => setStep(id)} key={`stepIndicator${id}`}>
            {currentStep === id ? <StepIconActive /> : <StepIconInactive />}
          </a>
        ))}
      </div>
    </>
  );
}

function StepIconInactive() {
  return (
    <div className={styles.stepIconHolder}>
      <svg
        className={styles.svgCircleHolder}
        width="8"
        height="8"
        viewBox="0 0 8 8"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <circle r="4" transform="matrix(-1 0 0 1 4 4)" fill="var(--none)" />
      </svg>
    </div>
  );
}

function StepIconActive() {
  return (
    <div className={styles.stepIconHolder}>
      <svg
        className={styles.svgCircleHolder}
        width="12"
        height="12"
        viewBox="0 0 12 12"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <circle r="6" transform="matrix(-1 0 0 1 6 6)" fill="var(--noneActive)" />
      </svg>
    </div>
  );
}
