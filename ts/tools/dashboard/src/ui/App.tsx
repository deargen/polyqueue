import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { DashboardPage } from "./pages/DashboardPage.tsx"

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 0,
      retry: 1,
    },
  },
})

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <DashboardPage />
    </QueryClientProvider>
  )
}
