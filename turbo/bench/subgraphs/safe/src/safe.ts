import { Address, BigInt, Bytes, ethereum } from "@graphprotocol/graph-ts";
import {
  AddedOwner,
  AddedOwner1,
  ChangedFallbackHandler,
  ChangedFallbackHandler1,
  ChangedGuard,
  ChangedGuard1,
  ChangedMasterCopy,
  ChangedModuleGuard,
  ChangedThreshold,
  DisabledModule,
  DisabledModule1,
  EnabledModule,
  EnabledModule1,
  ExecutionFailure,
  ExecutionFailure1,
  ExecutionSuccess,
  ExecutionSuccess1,
  RemovedOwner,
  RemovedOwner1,
  SafeModuleTransaction,
  SafeMultiSigTransaction,
  SafeReceived,
  SafeSetup as SafeSetupEvent,
} from "../generated/templates/Safe/Safe";
import { SafeEvent, SafeSetup } from "../generated/schema";

/** Builds a JSON object of event arguments, in ABI order. */
class Args {
  parts: Array<string> = [];
  str(key: string, value: string): Args {
    this.parts.push('"' + key + '":"' + value + '"');
    return this;
  }
  address(key: string, value: Address): Args {
    return this.str(key, value.toHexString());
  }
  bytes(key: string, value: Bytes): Args {
    return this.str(key, value.toHexString());
  }
  big(key: string, value: BigInt): Args {
    return this.str(key, value.toString());
  }
  small(key: string, value: i32): Args {
    this.parts.push('"' + key + '":' + value.toString());
    return this;
  }
  toString(): string {
    return "{" + this.parts.join(",") + "}";
  }
}

function record(event: ethereum.Event, name: string, args: Args): void {
  let row = new SafeEvent(event.transaction.hash.concatI32(event.logIndex.toI32()));
  row.proxy = event.address;
  row.name = name;
  row.args = args.toString();
  row.timestamp = event.block.timestamp.toI32();
  row.save();
}

export function handleSafeSetup(event: SafeSetupEvent): void {
  let row = new SafeSetup(event.transaction.hash.concatI32(event.logIndex.toI32()));
  row.proxy = event.address;
  row.initiator = event.params.initiator;
  row.threshold = event.params.threshold;
  row.timestamp = event.block.timestamp.toI32();
  row.save();
}

export function handleSafeReceived(event: SafeReceived): void {
  record(event, "SafeReceived", new Args().address("sender", event.params.sender).big("value", event.params.value));
}

export function handleSafeModuleTransaction(event: SafeModuleTransaction): void {
  let p = event.params;
  record(
    event,
    "SafeModuleTransaction",
    new Args().address("module", p.module).address("to", p.to).big("value", p.value).bytes("data", p.data).small("operation", p.operation),
  );
}

export function handleSafeMultiSigTransaction(event: SafeMultiSigTransaction): void {
  let p = event.params;
  record(
    event,
    "SafeMultiSigTransaction",
    new Args()
      .address("to", p.to)
      .big("value", p.value)
      .bytes("data", p.data)
      .small("operation", p.operation)
      .big("safeTxGas", p.safeTxGas)
      .big("baseGas", p.baseGas)
      .big("gasPrice", p.gasPrice)
      .address("gasToken", p.gasToken)
      .address("refundReceiver", p.refundReceiver)
      .bytes("signatures", p.signatures)
      .bytes("additionalInfo", p.additionalInfo),
  );
}

export function handleExecutionSuccess(event: ExecutionSuccess): void {
  record(event, "ExecutionSuccess", new Args().bytes("txHash", event.params.txHash).big("payment", event.params.payment));
}

export function handleExecutionSuccessIndexed(event: ExecutionSuccess1): void {
  record(event, "ExecutionSuccess", new Args().bytes("txHash", event.params.txHash).big("payment", event.params.payment));
}

export function handleExecutionFailure(event: ExecutionFailure): void {
  record(event, "ExecutionFailure", new Args().bytes("txHash", event.params.txHash).big("payment", event.params.payment));
}

export function handleExecutionFailureIndexed(event: ExecutionFailure1): void {
  record(event, "ExecutionFailure", new Args().bytes("txHash", event.params.txHash).big("payment", event.params.payment));
}

export function handleChangedThreshold(event: ChangedThreshold): void {
  record(event, "ChangedThreshold", new Args().big("threshold", event.params.threshold));
}

export function handleChangedMasterCopy(event: ChangedMasterCopy): void {
  record(event, "ChangedMasterCopy", new Args().address("singleton", event.params.singleton));
}

export function handleChangedFallbackHandler(event: ChangedFallbackHandler): void {
  record(event, "ChangedFallbackHandler", new Args().address("handler", event.params.handler));
}

export function handleChangedFallbackHandlerIndexed(event: ChangedFallbackHandler1): void {
  record(event, "ChangedFallbackHandler", new Args().address("handler", event.params.handler));
}

export function handleChangedGuard(event: ChangedGuard): void {
  record(event, "ChangedGuard", new Args().address("guard", event.params.guard));
}

export function handleChangedGuardIndexed(event: ChangedGuard1): void {
  record(event, "ChangedGuard", new Args().address("guard", event.params.guard));
}

export function handleChangedModuleGuard(event: ChangedModuleGuard): void {
  record(event, "ChangedModuleGuard", new Args().address("moduleGuard", event.params.moduleGuard));
}

export function handleEnabledModule(event: EnabledModule): void {
  record(event, "EnabledModule", new Args().address("module", event.params.module));
}

export function handleEnabledModuleIndexed(event: EnabledModule1): void {
  record(event, "EnabledModule", new Args().address("module", event.params.module));
}

export function handleDisabledModule(event: DisabledModule): void {
  record(event, "DisabledModule", new Args().address("module", event.params.module));
}

export function handleDisabledModuleIndexed(event: DisabledModule1): void {
  record(event, "DisabledModule", new Args().address("module", event.params.module));
}

export function handleAddedOwner(event: AddedOwner): void {
  record(event, "AddedOwner", new Args().address("owner", event.params.owner));
}

export function handleAddedOwnerIndexed(event: AddedOwner1): void {
  record(event, "AddedOwner", new Args().address("owner", event.params.owner));
}

export function handleRemovedOwner(event: RemovedOwner): void {
  record(event, "RemovedOwner", new Args().address("owner", event.params.owner));
}

export function handleRemovedOwnerIndexed(event: RemovedOwner1): void {
  record(event, "RemovedOwner", new Args().address("owner", event.params.owner));
}
