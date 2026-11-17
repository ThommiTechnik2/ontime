import type { AutomationDTO, OBSStepAction } from 'ontime-types';
import {
  type Control,
  type UseFormRegister,
  type UseFormSetValue,
  type UseFormWatch,
  useFieldArray,
} from 'react-hook-form';
import { IoAdd, IoTrash } from 'react-icons/io5';

import Button from '../../../../common/components/buttons/Button';
import IconButton from '../../../../common/components/buttons/IconButton';
import Input from '../../../../common/components/input/input/Input';
import Select from '../../../../common/components/select/Select';
import * as Panel from '../../panel-utils/PanelUtils';
import type { OutputErrors } from './automationUtils';
import TemplateInput from './template-input/TemplateInput';

import style from './AutomationForm.module.scss';

const stepOptions: { value: OBSStepAction; label: string; placeholder: string }[] = [
  { value: 'preview-scene', label: 'Set preview scene', placeholder: '{{eventNow.title}}' },
  { value: 'program-scene', label: 'Set program scene', placeholder: '{{eventNow.title}}' },
  { value: 'transition', label: 'Set transition', placeholder: '{{eventNow.custom.transition}}' },
  { value: 'transition-duration', label: 'Set transition duration (ms)', placeholder: '1000' },
  { value: 'studio-transition', label: 'Take (studio mode transition)', placeholder: '' },
  { value: 'wait', label: 'Wait (ms)', placeholder: '1000' },
  {
    value: 'wait-until-remaining',
    label: 'Wait until time left in event (ms)',
    placeholder: '{{eventNext.custom.transitionDuration}}',
  },
];

interface ObsOutputFormProps {
  index: number;
  control: Control<AutomationDTO>;
  register: UseFormRegister<AutomationDTO>;
  setValue: UseFormSetValue<AutomationDTO>;
  watch: UseFormWatch<AutomationDTO>;
  rowErrors?: OutputErrors;
}

export default function ObsOutputForm({ index, control, register, setValue, watch, rowErrors }: ObsOutputFormProps) {
  const {
    fields: steps,
    append,
    remove,
    move,
  } = useFieldArray({
    control,
    name: `outputs.${index}.steps`,
  });

  return (
    <>
      <label>
        OBS host
        <Input
          {...register(`outputs.${index}.targetIP`, { required: { value: true, message: 'Required field' } })}
          fluid
          placeholder='127.0.0.1'
        />
        <Panel.Error>{rowErrors?.targetIP?.message}</Panel.Error>
      </label>
      <label>
        WebSocket port
        <Input
          {...register(`outputs.${index}.targetPort`, {
            required: { value: true, message: 'Required field' },
            setValueAs: (value) => (value === '' ? 0 : Number(value)),
            max: { value: 65535, message: 'Port must be within range 1 - 65535' },
            min: { value: 1, message: 'Port must be within range 1 - 65535' },
          })}
          fluid
          type='number'
          maxLength={5}
          placeholder='4455'
        />
        <Panel.Error>{rowErrors?.targetPort?.message}</Panel.Error>
      </label>
      <label>
        Password
        <Input
          {...register(`outputs.${index}.password`)}
          fluid
          type='password'
          autoComplete='off'
          placeholder='Leave empty without authentication'
        />
      </label>

      <div className={style.spanFull}>
        <Panel.Description>
          Steps run in OBS one after the other. Steps whose value is empty are skipped. Use &quot;Wait until time
          left&quot; to finish a transition exactly at the end of the running event: the following steps wait until the
          event has that much time left, eg: the duration of the next transition. An empty value waits for the end of
          the event.
        </Panel.Description>
      </div>

      {steps.map((step, stepIndex) => {
        const actionPath = `outputs.${index}.steps.${stepIndex}.action` as const;
        const valuePath = `outputs.${index}.steps.${stepIndex}.value` as const;
        const action = watch(actionPath);
        const option = stepOptions.find((candidate) => candidate.value === action);
        const needsValue = action !== 'studio-transition';

        return (
          <div key={step.id} className={`${style.spanFull} ${style.obsStep}`}>
            <span className={style.obsStepIndex}>{stepIndex + 1}</span>
            <Select<OBSStepAction>
              value={action}
              onValueChange={(value) => {
                if (value === null) return;
                setValue(actionPath, value, { shouldDirty: true });
              }}
              options={stepOptions.map(({ value, label }) => ({ value, label }))}
              aria-label='OBS step'
            />
            {needsValue ? (
              <TemplateInput
                {...register(valuePath)}
                value={watch(valuePath)}
                fluid
                placeholder={option?.placeholder}
              />
            ) : (
              <span />
            )}
            <div className={style.obsStepButtons}>
              <Button
                variant='ghosted-white'
                disabled={stepIndex === 0}
                onClick={() => move(stepIndex, stepIndex - 1)}
                aria-label='Move step up'
              >
                ↑
              </Button>
              <IconButton aria-label='Delete step' variant='ghosted-destructive' onClick={() => remove(stepIndex)}>
                <IoTrash />
              </IconButton>
            </div>
          </div>
        );
      })}

      <div className={style.spanFull}>
        <Button onClick={() => append({ action: 'preview-scene', value: '' })}>
          Add step <IoAdd />
        </Button>
      </div>
    </>
  );
}
