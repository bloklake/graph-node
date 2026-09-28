import { BigInt } from "@graphprotocol/graph-ts";
import { Approval, ERC20 } from "../generated/TokenUSDT/ERC20";
import { ApprovalEvent, TokenAllowance } from "../generated/schema";

export function handleApproval(event: Approval): void {
  let token = event.address;
  let owner = event.params.owner;
  let spender = event.params.spender;
  let value = event.params.value;
  // A revoked approval is zero whatever the token reports: no read.
  let current = value.isZero() ? BigInt.zero() : ERC20.bind(token).allowance(owner, spender);

  let row = new ApprovalEvent(event.transaction.hash.concatI32(event.logIndex.toI32()));
  row.token = token;
  row.owner = owner;
  row.spender = spender;
  row.approved = value;
  row.allowance = current;
  row.timestamp = event.block.timestamp.toI32();
  row.save();

  let allowance = new TokenAllowance(token.concat(owner).concat(spender));
  allowance.token = token;
  allowance.owner = owner;
  allowance.spender = spender;
  allowance.allowance = current;
  allowance.save();
}
