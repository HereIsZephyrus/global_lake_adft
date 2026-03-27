export default function ErrorBox({ msg }: { msg: string }) {
  return (
    <div style={{
      background: '#3b1f1f', border: '1px solid #ef4444',
      borderRadius: 8, padding: '12px 16px', color: '#fca5a5',
      marginBottom: 16, fontSize: 13,
    }}>
      ⚠ {msg}
    </div>
  )
}
