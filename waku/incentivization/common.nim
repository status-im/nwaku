import std/options

import waku/incentivization/[rpc, eligibility_manager]

proc init*(T: type EligibilityStatus, isEligible: bool): T =
  if isEligible:
    EligibilityStatus(statusCode: uint32(200), statusDesc: some("OK"))
  else:
    EligibilityStatus(statusCode: uint32(402), statusDesc: some("Payment Required"))
