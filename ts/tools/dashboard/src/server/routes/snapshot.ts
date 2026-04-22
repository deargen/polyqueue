import { querySnapshot } from "../queries/snapshot.ts"

export async function handleSnapshot(_req: Request): Promise<Response> {
  const data = await querySnapshot()
  return new Response(JSON.stringify(data), {
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  })
}
