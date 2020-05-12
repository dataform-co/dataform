import {
  FileInput as BlueprintFileInput,
  H5,
  HTMLInputProps,
  IFileInputProps as IBlueprintFileInputProps,
  IInputGroupProps,
  InputGroup,
  ITagInputProps,
  TagInput
} from "@blueprintjs/core";
import * as styles from "df/components/forms.css";
import * as React from "react";
import { useState } from "react";

export interface IValidationRule<T> {
  predicate: (val: T) => boolean;
  message: string;
}

export class ValidationRules {
  public static required(name?: string): IValidationRule<string> {
    return {
      predicate: v => !!v,
      message: `${name || "Field"} is required.`
    };
  }

  public static email(): IValidationRule<string> {
    // from: https://emailregex.com/
    // tslint:disable-next-line: tsr-detect-unsafe-regexp
    const emailRegex = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    return {
      predicate: v => !v || emailRegex.test(v),
      message: "One of the emails is in an invalid format."
    };
  }
}

/**
 * Creates a strongly typed state object form managing forms.
 *
 * ```tsx
 * const form = useForm({ email: { default: "", rules: EMAIL_REGEX_RULE }});
 * return (
 *   <>
 *     <input value={form.email.value()} onChange={e => form.email.set(e.target.value)} />
 *     <span>{form.email.errors()}</span>
 *     <button disabled={form.valid()} onClick={() => doSomething(form.data())} />
 *   </>
 * );
 * ```
 */
export function useForm<T extends IUseForm>(form: T): IFormObject<T> & IFormMethods<T> {
  const items = Object.keys(form).reduce(
    (acc, curr) => ({ ...acc, [curr]: new FormItemState(form[curr]) }),
    {} as IFormObject<T>
  );
  const itemValues = Object.values(items);
  return {
    ...items,
    valid: () => itemValues.every(item => item.valid()),
    data: () =>
      Object.keys(items).reduce(
        (acc, curr) => ({ ...acc, [curr]: items[curr].value() }),
        {} as IFormData<T>
      ),
    showErrors: () => itemValues.forEach(item => item.showErrors())
  };
}

export interface IUseFormItem<T> {
  default: T;
  rules?: IValidationRule<T> | Array<IValidationRule<T>>;
}

interface IUseForm {
  [key: string]: IUseFormItem<any>;
}

type IFormItemDataType<T extends IUseFormItem<any>> = T extends IUseFormItem<infer U> ? U : never;

type IFormObject<T extends IUseForm> = {
  [P in keyof T]: FormItemState<IFormItemDataType<T[P]>>;
};

type IFormData<T extends IUseForm> = {
  [P in keyof T]: IFormItemDataType<T[P]>;
};

interface IFormMethods<T extends IUseForm> {
  valid: () => boolean;
  data: () => IFormData<T>;
  showErrors: () => void;
}

class FormItemState<T> {
  private valueState: [T, React.Dispatch<T>];
  private errorsState: [string[] | false, React.Dispatch<string[] | false>];
  private rules: Array<IValidationRule<T>>;

  constructor(value: IUseFormItem<T>) {
    this.valueState = useState(value.default);
    this.errorsState = useState<string[] | false>(false);
    this.rules = value.rules instanceof Array ? value.rules : value.rules ? [value.rules] : [];
  }

  public valid = () => {
    const validationErrors = this.rules
      .filter(rule => !rule.predicate(this.valueState[0]))
      .map(rule => rule.message);
    return validationErrors && validationErrors.length === 0;
  };

  public value = () => {
    return this.valueState[0];
  };

  public errors = () => {
    return !this.errorsState[0] ? [] : this.errorsState[0];
  };

  public set = (value: T) => {
    const [_, setState] = this.valueState;
    const [__, setErrors] = this.errorsState;
    const validationErrors = this.rules
      .filter(rule => !rule.predicate(value))
      .map(rule => rule.message);

    const ___ = Promise.all([setState(value), setErrors(validationErrors)]);
  };

  public showErrors = () => this.set(this.valueState[0]);
}

interface IFormProps extends React.HTMLProps<HTMLFormElement> {
  onSubmit: () => void;
}

export function Form({ onSubmit, children, ...rest }: React.PropsWithChildren<IFormProps>) {
  return (
    <form
      {...rest}
      onSubmit={(e: React.FormEvent<HTMLFormElement>) => {
        // prevent page reload
        e.preventDefault();
        onSubmit();
      }}
    >
      {children}
    </form>
  );
}

interface IItemProps {
  name: string | React.ReactElement<any>;
  description?: string | React.ReactElement<any>;
  errors?: string[];
}

export const FormItem = (props: React.PropsWithChildren<IItemProps>) => (
  <div style={{ marginTop: "20px" }}>
    <div>
      <H5>{props.name}</H5>
      <div className="bp3-text-muted" style={{ marginBottom: "10px" }}>
        {props.description}
      </div>
    </div>
    <div>{props.children}</div>
    {props.errors && <ValidationErrors errors={props.errors} />}
  </div>
);

interface IInputGroupWithValidationProps {
  validationRules?: Array<IValidationRule<string>>;
}

/**
 * @deprecated Use `useForm`, or `ValidationErrors` directly.
 */
export const InputGroupWithValidation = ({
  validationRules,
  onChange,
  onBlur,
  ...rest
}: IInputGroupWithValidationProps & IInputGroupProps & HTMLInputProps) => {
  const { value } = useForm({
    value: {
      default: rest.value || rest.defaultValue,
      rules: [
        ...(rest.required ? [ValidationRules.required(rest.name)] : []),
        ...(validationRules || [])
      ]
    }
  });

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    value.set(e.target.value);
    if (onChange) {
      onChange(e);
    }
  };

  return (
    <>
      <InputGroup
        {...rest}
        onChange={handleChange}
        onBlur={e => {
          value.showErrors();
          if (onBlur) {
            onBlur(e);
          }
        }}
      />
      <ValidationErrors errors={value.errors()} />
    </>
  );
};

interface IFileInputProps extends IBlueprintFileInputProps {
  value?: string | string[] | number;
}

export const FileInput = (props: IFileInputProps & React.LabelHTMLAttributes<HTMLLabelElement>) => {
  return (
    <BlueprintFileInput
      {...props}
      text={props.value ? <span>{props.value}</span> : <span>Choose file...</span>}
      hasSelection={!!props.value}
    />
  );
};

interface IFileInputWithValidationProps {
  validationRules?: Array<IValidationRule<string>>;
  name: string;
  required: boolean;
}

/**
 * @deprecated Use `useForm`, or `ValidationErrors` directly.
 */
export const FileInputWithValidation = ({
  validationRules,
  onInputChange,
  onBlur,
  ...rest
}: IFileInputWithValidationProps & IFileInputProps) => {
  const { fileName } = useForm({
    fileName: {
      default: rest.value || rest.defaultValue,
      rules: [
        ...(rest.required ? [ValidationRules.required(rest.name)] : []),
        ...(validationRules || [])
      ]
    }
  });
  return (
    <>
      <FileInput
        {...rest}
        value={fileName.value()}
        onInputChange={e => {
          const file = (e.target as HTMLInputElement).files[0];
          fileName.set(file.name);
          if (onInputChange) {
            onInputChange(e);
          }
        }}
        onBlur={(e: React.FocusEvent<HTMLLabelElement>) => {
          fileName.showErrors();
          if (onBlur) {
            onBlur(e);
          }
        }}
      />

      <ValidationErrors errors={fileName.errors()} />
    </>
  );
};

interface ITagInputWithValidationProps extends ITagInputProps {
  validationRules?: Array<IValidationRule<string>>;
}

/**
 * @deprecated Use `useForm`, or `ValidationErrors` directly.
 */
export const TagInputWithValidation = ({
  validationRules,
  values,
  onChange,
  ...rest
}: ITagInputWithValidationProps) => {
  const form = useForm({
    values: {
      default: values || values,
      rules: [...(validationRules || [])]
    }
  });

  const handleChange = (newValues: string[]) => {
    if (onChange) {
      onChange(newValues);
    }
  };

  return (
    <>
      <TagInput {...rest} onChange={handleChange} values={values} />
      <ValidationErrors errors={form.values.errors()} />
    </>
  );
};

/**
 * Renders a list of string errors produced by validation rules (or otherwise).
 */
export const ValidationErrors = ({ errors }: { errors: string[] }) => (
  <div className={styles.validationErrors}>
    {errors.map(error => (
      <span key={error}>{error}</span>
    ))}
  </div>
);
