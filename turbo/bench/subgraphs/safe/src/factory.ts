import { ethereum, Address } from "@graphprotocol/graph-ts";
import { ProxyCreation as ProxyCreation130 } from "../generated/Factory130/ProxyFactory130";
import { ProxyCreation as ProxyCreation141 } from "../generated/Factory141/ProxyFactory141";
import { Safe as SafeTemplate } from "../generated/templates";
import { Safe } from "../generated/schema";

function created(event: ethereum.Event, proxy: Address, singleton: Address): void {
  SafeTemplate.create(proxy);
  let row = new Safe(event.transaction.hash.concatI32(event.logIndex.toI32()));
  row.proxy = proxy;
  row.singleton = singleton;
  row.timestamp = event.block.timestamp.toI32();
  row.save();
}

export function handleProxyCreation130(event: ProxyCreation130): void {
  created(event, event.params.proxy, event.params.singleton);
}

export function handleProxyCreation141(event: ProxyCreation141): void {
  created(event, event.params.proxy, event.params.singleton);
}
